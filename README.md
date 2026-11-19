# Taller Spark

ETL local de ventas que lee cuatro CSV y un lote JSONL, valida los registros, calcula el importe neto de cada línea y resume las ventas por mes, país, canal y categoría.

## Cómo correrlo

Con Python 3.12 y Java 17 o 21, ejecuta desde la raíz del repositorio:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python taller.py
```

Spark usa `local[*]` y escribe en `output/ventas_mensuales/` y `output/cuarentena/`. Cada salida contiene un archivo Parquet de datos porque ambos resultados son pequeños.

## Modelo y decisiones

Los archivos de `datos/` se exportaron de las seis hojas del Excel porque faltaba el ZIP del taller. Spark lee los CSV como texto y el lote JSONL con las mismas columnas. El perfil inicial muestra esquemas, claves distintas y campos vacíos. Convierto explícitamente los tipos que intervienen en las validaciones y el cálculo.

Clientes y productos se identifican por `customer_id` y `product_id`. Cada pedido tiene un `order_id` y referencia un cliente. Cada línea tiene un `line_id` y referencia un pedido y un producto.

| Hallazgo | Decisión |
| --- | --- |
| Copias exactas de C0020 y O00012 | Conservo una fila y cuento las copias retiradas. Si una clave de cliente, producto o línea tiene variantes distintas, las pongo en cuarentena. |
| Estado `deliverd` en O00030 | Lo corrijo a `delivered` y reporto el cambio. |
| Correo mal formado en C0007 y correo ausente en C0013 | Reporto ambos y conservo los clientes porque su país se puede usar para agrupar. |
| Cliente inexistente en O00007, cliente ausente en O00011 y fecha imposible en O00025 | Envío a cuarentena los pedidos y las líneas que dependen de ellos. |
| L0000003 referencia un pedido inexistente y L0000004, un producto inexistente | Envío las líneas a cuarentena porque falta una referencia necesaria. |
| L0000001 y L0000002 tienen cantidad no positiva, L0000005 tiene descuento 1.2, P088 tiene precio negativo y P089 no tiene precio | Exijo cantidades enteras positivas, descuentos entre 0 y 1 antes de redondear, y precios positivos. Envío a cuarentena los registros inválidos y las líneas que requieren un producto descartado. |

La cuarentena guarda cada registro excluido con el primer motivo aplicable. Incluyo todos los estados del pedido y los productos inactivos con precio válido porque la consigna no pide filtrar por estado ni por vigencia.

## Cálculo y controles

Parto de las líneas. Con una sola fila válida por pedido, cliente y producto, los joins permiten agregar sin multiplicar los importes. Calculo `cantidad × precio × (1 − descuento)` con precio y descuento de dos decimales e importe de cuatro. Obtengo el mes de `order_ts` en UTC y el país del cliente.

| Control | Resultado con los datos incluidos |
| --- | ---: |
| Filas válidas, claves `line_id` distintas e importes calculados | 39.386 en los tres conteos |
| Suma del detalle y suma del resumen | 95703917.8750 en ambas |
| Grupos en el Parquet mensual | 208 |
| Registros en el Parquet de cuarentena | 845 |

Las 40.226 líneas de entrada se reparten en 39.386 válidas y 840 en cuarentena. Los otros cinco registros de la cuarentena corresponden a tres pedidos y dos productos.

## Incremental

Combino el histórico y el lote, retiro las copias exactas y elijo el mayor `updated_at` por `order_id`. Si hay un empate de fecha, ordeno por los demás campos para resolverlo de forma repetible. O00005 conserva la versión con `updated_at` de `2026-03-05T12:00:00Z`. El mes sigue saliendo de `order_ts`.

El lote trae 100 pedidos nuevos, una versión tardía de un pedido existente y 200 líneas nuevas. Deduplico las líneas por sus copias exactas y verifico que `line_id` sea único en el detalle válido. Cada ejecución reconstruye y sobrescribe los Parquet, de modo que repetir el mismo lote conserva el mismo contenido.

Al duplicar ambos JSONL en una copia del proyecto, el conteo siguió en 20.100 pedidos únicos antes de validar referencias. Los dos Parquet coincidieron fila por fila con la ejecución de un solo lote.
