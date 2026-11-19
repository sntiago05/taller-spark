from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType


def perfilar(df, nombre, clave):
    print(f"\nPerfil de {nombre}")
    df.printSchema()
    df.agg(
        F.count("*").alias("filas"),
        F.countDistinct(clave).alias("claves_distintas"),
        *[
            F.count(F.when(F.col(c).isNull() | (F.trim(F.col(c)) == ""), 1)).alias(c)
            for c in df.columns
        ],
    ).show(vertical=True)


def quitar_copias(df, nombre, filas):
    unicos = df.dropDuplicates().cache()
    print(f"{nombre}: {filas - unicos.count()} copias exactas retiradas")
    return unicos


def clave_repetida(clave):
    return F.count("*").over(Window.partitionBy(clave)) > 1


def separar(df, nombre, motivo, cuarentenas):
    revisado = df.withColumn("motivo", motivo).cache()
    columnas = [F.col(c) for c in df.columns]
    cuarentenas.append(
        revisado.where(F.col("motivo").isNotNull()).select(
            F.lit(nombre).alias("tabla"),
            "motivo",
            F.to_json(F.struct(*columnas), {"ignoreNullFields": "false"}).alias("registro"),
        )
    )
    return revisado.where(F.col("motivo").isNull()).drop("motivo")


def vacio(columna):
    return F.col(columna).isNull() | (F.trim(F.col(columna)) == "")


def ejecutar(spark):
    def csv(nombre):
        return spark.read.option("header", True).option("inferSchema", False).csv(
            f"datos/{nombre}.csv"
        )

    def jsonl(nombre, base):
        esquema = StructType([StructField(c, StringType()) for c in base.columns])
        return spark.read.schema(esquema).json(f"datos/incremental/{nombre}.jsonl")

    clientes = csv("customers")
    productos = csv("products")
    pedidos = csv("orders")
    lineas = csv("order_items")
    pedidos_lote = jsonl("orders_incremental", pedidos)
    lineas_lote = jsonl("order_items_incremental", lineas)
    cuarentenas = []

    clientes = clientes.cache()
    productos = productos.cache()
    pedidos = pedidos.cache()
    lineas = lineas.cache()
    pedidos_lote = pedidos_lote.cache()
    lineas_lote = lineas_lote.cache()

    for df, nombre, clave in [
        (clientes, "clientes", "customer_id"),
        (productos, "productos", "product_id"),
        (pedidos, "pedidos históricos", "order_id"),
        (lineas, "líneas históricas", "line_id"),
        (pedidos_lote, "pedidos del lote", "order_id"),
        (lineas_lote, "líneas del lote", "line_id"),
    ]:
        perfilar(df, nombre, clave)

    clientes = quitar_copias(clientes, "clientes", clientes.count())
    clientes = clientes.withColumn("alta", F.expr("try_cast(signup_date as date)"))
    clientes.agg(
        F.count(F.when(vacio("email"), 1)).alias("correo_ausente"),
        F.count(F.when(~vacio("email") & ~F.col("email").rlike(r"^[^@\s]+@[^@\s]+\.[^@\s]+$"), 1))
        .alias("correo_mal_formado"),
        F.count(F.when(F.col("alta").isNull(), 1)).alias("fecha_alta_invalida"),
    ).show()
    clientes = separar(
        clientes,
        "clientes",
        F.when(vacio("customer_id"), "cliente sin clave")
        .when(clave_repetida("customer_id"), "clave de cliente en conflicto")
        .when(vacio("country"), "país ausente"),
        cuarentenas,
    ).select(F.col("customer_id").alias("cliente_valido"), "country")

    productos = quitar_copias(productos, "productos", productos.count())
    productos = productos.withColumn("precio", F.expr("try_cast(unit_price as decimal(14,2))"))
    productos = separar(
        productos,
        "productos",
        F.when(vacio("product_id"), "producto sin clave")
        .when(clave_repetida("product_id"), "clave de producto en conflicto")
        .when(vacio("category"), "categoría ausente")
        .when(F.col("precio").isNull() | (F.col("precio") <= 0), "precio inválido"),
        cuarentenas,
    ).select(F.col("product_id").alias("producto_valido"), "category", "precio")

    todos_pedidos = pedidos.unionByName(pedidos_lote)
    pedidos = quitar_copias(todos_pedidos, "pedidos", todos_pedidos.count())
    pedidos = pedidos.withColumn("actualizado", F.expr("try_cast(updated_at as timestamp)"))
    pedidos = separar(
        pedidos,
        "pedidos",
        F.when(vacio("order_id"), "pedido sin clave")
        .when(F.col("actualizado").isNull(), "actualización sin fecha válida"),
        cuarentenas,
    )

    # Ante fechas iguales, ordenar por los demás campos da un resultado repetible.
    versiones = Window.partitionBy("order_id").orderBy(
        F.desc("actualizado"), "updated_at", "order_ts", "customer_id", "status", "channel"
    )
    ordenados = pedidos.withColumn("version", F.row_number().over(versiones))
    print(f"Versiones anteriores de pedidos: {ordenados.where('version > 1').count()}")
    pedidos = ordenados.where("version = 1").drop("version")
    print(f"Pedidos únicos antes de validar los datos de la versión vigente: {pedidos.count()}")
    print(f"Estados deliverd corregidos: {pedidos.where(F.col('status') == 'deliverd').count()}")
    pedidos = pedidos.withColumn(
        "status", F.when(F.col("status") == "deliverd", "delivered").otherwise(F.col("status"))
    ).withColumn("fecha_pedido", F.expr("try_cast(order_ts as timestamp)"))
    pedidos = pedidos.join(clientes, pedidos.customer_id == clientes.cliente_valido, "left")
    pedidos = separar(
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

    todas_lineas = lineas.unionByName(lineas_lote)
    lineas = quitar_copias(todas_lineas, "líneas", todas_lineas.count())
    lineas = (
        lineas.withColumn("cantidad", F.expr("try_cast(quantity as int)"))
        .withColumn("descuento", F.expr("try_cast(discount_pct as decimal(5,2))"))
    )
    lineas = separar(
        lineas,
        "líneas",
        F.when(vacio("line_id"), "línea sin clave")
        .when(clave_repetida("line_id"), "clave de línea en conflicto")
        .when(F.col("cantidad").isNull() | (F.col("cantidad") <= 0), "cantidad inválida")
        .when(F.col("descuento").isNull() | ~F.col("descuento").between(0, 1), "descuento inválido"),
        cuarentenas,
    )
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
    detalle = detalle.withColumn(
        "importe_neto",
        (F.col("cantidad") * F.col("precio") * (1 - F.col("descuento"))).cast("decimal(28,4)"),
    ).cache()
    resumen = detalle.groupBy("mes", "country", "channel", "category").agg(
        F.sum("importe_neto").alias("ventas_netas")
    ).cache()

    control = detalle.agg(
        F.count("*").alias("filas"),
        F.countDistinct("line_id").alias("lineas_distintas"),
        F.sum("importe_neto").alias("total"),
    ).first()
    total_resumen = resumen.agg(F.sum("ventas_netas")).first()[0]
    print(f"QA 1 - Filas del detalle: {control.filas}, line_id distintos: {control.lineas_distintas}")
    print(f"QA 2 - Total detalle: {control.total}, total resumen: {total_resumen}")
    if control.filas == 0 or control.filas != control.lineas_distintas:
        raise ValueError("El detalle está vacío o tiene líneas repetidas.")
    if control.total is None or control.total != total_resumen:
        raise ValueError("El detalle y el resumen no reconcilian.")

    cuarentena = cuarentenas[0]
    for registros in cuarentenas[1:]:
        cuarentena = cuarentena.unionByName(registros)
    cuarentena = cuarentena.cache()
    cuarentena.groupBy("tabla", "motivo").count().orderBy("tabla", "motivo").show(truncate=False)
    resumen.orderBy("mes", "country", "channel", "category").show()
    resumen.write.mode("overwrite").parquet("output/ventas_mensuales")
    cuarentena.write.mode("overwrite").parquet("output/cuarentena")


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
