from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


def vacio(columna):
    return F.col(columna).isNull() | (F.trim(F.col(columna)) == "")


def perfilar(df, nombre, clave):
    print(f"\nPerfil de {nombre}")
    df.printSchema()
    df.agg(
        F.count("*").alias("filas"),
        F.countDistinct(clave).alias("claves_distintas"),
        *[
            F.count(F.when(vacio(columna), 1)).alias(columna)
            for columna in df.columns
        ],
    ).show(vertical=True)


def quitar_copias(df, nombre):
    unicos = df.dropDuplicates().cache()
    print(f"{nombre}: {df.count() - unicos.count()} copias exactas retiradas")
    return unicos


def clave_repetida(clave):
    return F.count("*").over(Window.partitionBy(clave)) > 1


def separar(df, nombre, motivo, cuarentenas):
    revisado = df.withColumn("motivo", motivo).cache()
    cuarentenas.append(
        revisado.where("motivo IS NOT NULL").select(
            F.lit(nombre).alias("tabla"),
            "motivo",
            F.to_json(F.struct(*df.columns), {"ignoreNullFields": "false"}).alias("registro"),
        )
    )
    return revisado.where("motivo IS NULL").drop("motivo")


def leer_y_perfilar(spark):
    def csv(nombre):
        return spark.read.csv(f"datos/{nombre}.csv", header=True).cache()

    def jsonl(nombre, base):
        return spark.read.schema(base.schema).json(f"datos/incremental/{nombre}.jsonl").cache()

    clientes = csv("customers")
    productos = csv("products")
    pedidos = csv("orders")
    lineas = csv("order_items")
    pedidos_lote = jsonl("orders_incremental", pedidos)
    lineas_lote = jsonl("order_items_incremental", lineas)
    for df, nombre, clave in [
        (clientes, "clientes", "customer_id"),
        (productos, "productos", "product_id"),
        (pedidos, "pedidos históricos", "order_id"),
        (lineas, "líneas históricas", "line_id"),
        (pedidos_lote, "pedidos del lote", "order_id"),
        (lineas_lote, "líneas del lote", "line_id"),
    ]:
        perfilar(df, nombre, clave)

    return clientes, productos, pedidos.unionByName(pedidos_lote), lineas.unionByName(lineas_lote)


def limpiar_clientes(clientes, cuarentenas):
    clientes = quitar_copias(clientes, "clientes")
    clientes = clientes.withColumn("alta", F.expr("try_cast(signup_date as date)"))
    formato_correo = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
    correo_mal_formado = ~vacio("email") & ~F.col("email").rlike(formato_correo)
    clientes.agg(
        F.count(F.when(vacio("email"), 1)).alias("correo_ausente"),
        F.count(F.when(correo_mal_formado, 1)).alias("correo_mal_formado"),
        F.count(F.when(F.col("alta").isNull(), 1)).alias("fecha_alta_invalida"),
    ).show()
    return separar(
        clientes,
        "clientes",
        F.when(vacio("customer_id"), "cliente sin clave")
        .when(clave_repetida("customer_id"), "clave de cliente en conflicto")
        .when(vacio("country"), "país ausente"),
        cuarentenas,
    ).select(F.col("customer_id").alias("cliente_valido"), "country")


def limpiar_productos(productos, cuarentenas):
    productos = quitar_copias(productos, "productos")
    productos = productos.withColumn("precio", F.expr("try_cast(unit_price as decimal(14,2))"))
    return separar(
        productos,
        "productos",
        F.when(vacio("product_id"), "producto sin clave")
        .when(clave_repetida("product_id"), "clave de producto en conflicto")
        .when(vacio("category"), "categoría ausente")
        .when(F.col("precio").isNull() | (F.col("precio") <= 0), "precio inválido"),
        cuarentenas,
    ).select(F.col("product_id").alias("producto_valido"), "category", "precio")


def elegir_pedidos_recientes(pedidos, cuarentenas):
    pedidos = quitar_copias(pedidos, "pedidos")
    pedidos = pedidos.withColumn("actualizado", F.expr("try_cast(updated_at as timestamp)"))
    pedidos = separar(
        pedidos,
        "pedidos",
        F.when(vacio("order_id"), "pedido sin clave")
        .when(F.col("actualizado").isNull(), "actualización sin fecha válida"),
        cuarentenas,
    )

    # row_number numera las versiones desde la más reciente. El resto desempata.
    versiones = Window.partitionBy("order_id").orderBy(
        F.desc("actualizado"), "updated_at", "order_ts", "customer_id", "status", "channel"
    )
    ordenados = pedidos.withColumn("version", F.row_number().over(versiones))
    print(f"Versiones anteriores de pedidos: {ordenados.where('version > 1').count()}")
    pedidos = ordenados.where("version = 1").drop("version")
    print(f"Pedidos únicos antes de validar los datos de la versión vigente: {pedidos.count()}")
    return pedidos


def limpiar_pedidos(pedidos, clientes, cuarentenas):
    pedidos = elegir_pedidos_recientes(pedidos, cuarentenas)
    print(f"Estados deliverd corregidos: {pedidos.where(F.col('status') == 'deliverd').count()}")
    pedidos = pedidos.withColumn(
        "status", F.when(F.col("status") == "deliverd", "delivered").otherwise(F.col("status"))
    ).withColumn("fecha_pedido", F.expr("try_cast(order_ts as timestamp)"))
    pedidos = pedidos.join(clientes, pedidos.customer_id == clientes.cliente_valido, "left")
    return separar(
        pedidos,
        "pedidos",
        F.when(vacio("customer_id"), "pedido sin cliente")
        .when(F.col("cliente_valido").isNull(), "cliente sin referencia válida")
        .when(F.col("fecha_pedido").isNull(), "fecha de pedido inválida")
        .when(vacio("channel"), "canal ausente"),
        cuarentenas,
    ).select(
        F.col("order_id").alias("pedido_valido"),
        F.date_format("fecha_pedido", "yyyy-MM").alias("mes"),
        "country",
        "channel",
    )


def limpiar_lineas(lineas, cuarentenas):
    lineas = quitar_copias(lineas, "líneas")
    lineas = (
        lineas.withColumn("cantidad", F.expr("try_cast(quantity as int)"))
        .withColumn("descuento", F.expr("try_cast(discount_pct as decimal(5,2))"))
    )
    # Comprobar el rango antes de redondear el descuento para el cálculo.
    descuento_original = F.expr("try_cast(discount_pct as decimal(38,30))")
    descuento_invalido = descuento_original.isNull() | ~descuento_original.between(0, 1)
    return separar(
        lineas,
        "líneas",
        F.when(vacio("line_id"), "línea sin clave")
        .when(clave_repetida("line_id"), "clave de línea en conflicto")
        .when(F.col("cantidad").isNull() | (F.col("cantidad") <= 0), "cantidad inválida")
        .when(descuento_invalido, "descuento inválido"),
        cuarentenas,
    )


def calcular_detalle(lineas, pedidos, productos, cuarentenas):
    con_pedido = lineas.join(pedidos, lineas.order_id == pedidos.pedido_valido, "left")
    con_producto = con_pedido.join(
        productos, con_pedido.product_id == productos.producto_valido, "left"
    )
    detalle = separar(
        con_producto,
        "líneas",
        F.when(F.col("pedido_valido").isNull(), "pedido sin referencia válida")
        .when(F.col("producto_valido").isNull(), "producto sin referencia válida"),
        cuarentenas,
    )
    return detalle.withColumn(
        "importe_neto",
        F.expr("cast(cantidad * precio * (1 - descuento) as decimal(28,4))"),
    )


def validar_totales(detalle, resumen):
    control = detalle.agg(
        F.count("*").alias("filas"),
        F.countDistinct("line_id").alias("lineas_distintas"),
        F.count("importe_neto").alias("importes_calculados"),
        F.sum("importe_neto").alias("total"),
    ).first()
    total_resumen = resumen.agg(F.sum("ventas_netas")).first()[0]
    print(f"QA 1 - Filas del detalle: {control.filas}, line_id distintos: {control.lineas_distintas}")
    print(f"QA 2 - Total detalle: {control.total}, total resumen: {total_resumen}")
    if control.filas == 0 or control.filas != control.lineas_distintas:
        raise ValueError("El detalle está vacío o tiene líneas repetidas.")
    if control.filas != control.importes_calculados:
        raise ValueError("Hay líneas válidas sin importe neto.")
    if control.total is None or control.total != total_resumen:
        raise ValueError("El detalle y el resumen no reconcilian.")


def guardar_resultados(resumen, cuarentenas):
    cuarentena = cuarentenas[0]
    for registros in cuarentenas[1:]:
        cuarentena = cuarentena.unionByName(registros)
    cuarentena = cuarentena.cache()
    cuarentena.groupBy("tabla", "motivo").count().orderBy("tabla", "motivo").show(truncate=False)
    resumen.orderBy("mes", "country", "channel", "category").show()
    resumen.coalesce(1).write.mode("overwrite").parquet("output/ventas_mensuales")
    cuarentena.coalesce(1).write.mode("overwrite").parquet("output/cuarentena")


def ejecutar(spark):
    clientes, productos, pedidos, lineas = leer_y_perfilar(spark)
    cuarentenas = []
    clientes = limpiar_clientes(clientes, cuarentenas)
    productos = limpiar_productos(productos, cuarentenas)
    pedidos = limpiar_pedidos(pedidos, clientes, cuarentenas)
    lineas = limpiar_lineas(lineas, cuarentenas)
    detalle = calcular_detalle(lineas, pedidos, productos, cuarentenas).cache()
    resumen = detalle.groupBy("mes", "country", "channel", "category").agg(
        F.sum("importe_neto").alias("ventas_netas")
    ).cache()
    validar_totales(detalle, resumen)
    guardar_resultados(resumen, cuarentenas)


if __name__ == "__main__":
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("taller-data-engineer")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        ejecutar(spark)
    finally:
        spark.stop()
