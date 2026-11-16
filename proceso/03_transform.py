# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Transform · Bronze → Silver
# MAGIC Reglas aplicadas (100 % PySpark DataFrame API):
# MAGIC | Tabla | Transformaciones |
# MAGIC |---|---|
# MAGIC | `dim_clientes` | trim / initcap, nulos → valores por defecto, validación y **enmascarado de email (UDF)**, deduplicación con `row_number()` |
# MAGIC | `dim_productos` | deduplicación, margen unitario, rango de precio con `when/otherwise` |
# MAGIC | `fact_ventas` | deduplicación de órdenes, parseo seguro de fechas, **broadcast join** con dimensiones, métricas monetarias, atributos de calendario (`create_map`), estado de envío, **MERGE incremental (upsert)** |
# MAGIC | `ventas_rechazadas` | cuarentena con el motivo del rechazo |

# COMMAND ----------

# MAGIC %run ./00_utilidades

# COMMAND ----------

from delta.tables import DeltaTable

dbutils.widgets.text("catalogo", "catalog_retail_dev")
dbutils.widgets.text("esquema_source", "bronze")
dbutils.widgets.text("esquema_sink", "silver")

catalogo = dbutils.widgets.get("catalogo")
src = f"{catalogo}.{dbutils.widgets.get('esquema_source')}"
snk = f"{catalogo}.{dbutils.widgets.get('esquema_sink')}"

# COMMAND ----------

# MAGIC %md ## 1. Dimensión clientes

# COMMAND ----------

@F.udf(returnType=StringType())
def enmascarar_email(email):
    """ana.quispe12@correo.pe -> an*********@correo.pe (protección de datos personales)."""
    if email is None or "@" not in email:
        return None
    usuario, dominio = email.split("@", 1)
    return usuario[:2] + "*" * max(len(usuario) - 2, 1) + "@" + dominio


def vacio_a_nulo(columna: str):
    return F.when(F.trim(F.col(columna)) == "", F.lit(None)).otherwise(F.trim(F.col(columna)))


ventana_cliente = Window.partitionBy("cliente_id").orderBy(F.col("fecha_ingesta").desc(),
                                                           F.col("email").desc_nulls_last())

df_clientes = (spark.table(f"{src}.clientes")
               .withColumn("cliente_id", F.upper(F.trim("cliente_id")))
               .filter(F.col("cliente_id").isNotNull())
               .withColumn("nombre", F.initcap(F.trim("nombre")))
               .withColumn("ciudad", F.initcap(F.trim("ciudad")))
               .withColumn("region", F.initcap(F.trim("region")))
               .withColumn("segmento", F.coalesce(vacio_a_nulo("segmento"), F.lit("Sin segmento")))
               .withColumn("email", F.lower(vacio_a_nulo("email")))
               .withColumn("email_valido", F.coalesce(F.col("email").rlike(REGEX_EMAIL), F.lit(False)))
               .withColumn("email_enmascarado", enmascarar_email(F.col("email")))
               .withColumn("fecha_registro", parsear_fecha("fecha_registro"))
               .withColumn("antiguedad_dias", F.datediff(F.current_date(), F.col("fecha_registro")))
               .withColumn("rn", F.row_number().over(ventana_cliente))
               .filter(F.col("rn") == 1)
               .drop("rn")
               .withColumn("fecha_proceso", F.current_timestamp()))

escribir_tabla(df_clientes, f"{snk}.dim_clientes")

# COMMAND ----------

# MAGIC %md ## 2. Dimensión productos

# COMMAND ----------

df_productos = (spark.table(f"{src}.productos")
                .dropDuplicates(["producto_id"])
                .withColumn("nombre_producto", F.trim("nombre_producto"))
                .withColumn("activo", F.coalesce(F.col("activo"), F.lit(True)))
                .withColumn("margen_unitario_pct",
                            F.when(F.col("precio_lista") > 0,
                                   F.round((F.col("precio_lista") - F.col("costo_unitario"))
                                           / F.col("precio_lista") * 100, 2)))
                .withColumn("rango_precio",
                            F.when(F.col("precio_lista") < 100, "Bajo (<100)")
                             .when(F.col("precio_lista") < 1000, "Medio (100-999)")
                             .otherwise("Alto (>=1000)"))
                .withColumn("fecha_proceso", F.current_timestamp()))

escribir_tabla(df_productos, f"{snk}.dim_productos")

# COMMAND ----------

# MAGIC %md ## 3. Hechos de ventas + reglas de calidad

# COMMAND ----------

dim_cli = spark.table(f"{snk}.dim_clientes").select("cliente_id", "segmento", "ciudad", "region")
dim_prod = spark.table(f"{snk}.dim_productos").select("producto_id", "nombre_producto", "categoria",
                                                      "subcategoria", "precio_lista", "costo_unitario")

dias_es = F.create_map([F.lit(x) for kv in {1: "Domingo", 2: "Lunes", 3: "Martes", 4: "Miércoles",
                                            5: "Jueves", 6: "Viernes", 7: "Sábado"}.items() for x in kv])

df_ventas = (spark.table(f"{src}.ventas")
             .dropDuplicates(["orden_id"])
             .withColumn("cliente_id", F.upper(F.trim("cliente_id")))
             .withColumn("producto_id", F.upper(F.trim("producto_id")))
             .withColumn("canal", F.initcap(F.trim("canal")))
             .withColumn("modo_envio", F.trim("modo_envio"))
             .withColumn("descuento", F.coalesce(F.col("descuento"), F.lit(0.0)))
             .withColumn("fecha_orden_dt", parsear_fecha("fecha_orden"))
             .withColumn("fecha_envio_dt", parsear_fecha("fecha_envio"))
             .join(F.broadcast(dim_prod), "producto_id", "left")
             .join(F.broadcast(dim_cli), "cliente_id", "left"))

df_ventas = df_ventas.withColumn(
    "motivo_rechazo",
    F.when(F.col("cliente_id").isNull(), "Cliente nulo")
     .when(F.col("region").isNull(), "Cliente inexistente")
     .when(F.col("nombre_producto").isNull(), "Producto inexistente")
     .when(F.col("cantidad").isNull() | (F.col("cantidad") <= 0), "Cantidad inválida")
     .when(F.col("fecha_orden_dt").isNull(), "Fecha de orden inválida")
     .when(~F.col("descuento").between(0, 0.9), "Descuento fuera de rango")
     .when(F.col("fecha_envio_dt") < F.col("fecha_orden_dt"), "Envío anterior a la orden"))

df_ventas.cache()

# COMMAND ----------

# Cuarentena
df_rechazadas = (df_ventas.filter(F.col("motivo_rechazo").isNotNull())
                 .withColumn("fecha_proceso", F.current_timestamp()))
escribir_tabla(df_rechazadas, f"{snk}.ventas_rechazadas")
display(df_rechazadas.groupBy("motivo_rechazo").count().orderBy(F.desc("count")))

# COMMAND ----------

df_fact = (df_ventas.filter(F.col("motivo_rechazo").isNull())
           .drop("fecha_orden", "fecha_envio")
           .withColumnRenamed("fecha_orden_dt", "fecha_orden")
           .withColumnRenamed("fecha_envio_dt", "fecha_envio")
           # calendario
           .withColumn("anio", F.year("fecha_orden"))
           .withColumn("mes", F.month("fecha_orden"))
           .withColumn("trimestre", F.quarter("fecha_orden"))
           .withColumn("dia_semana", dias_es[F.dayofweek("fecha_orden")])
           # métricas monetarias
           .withColumn("precio_unitario", F.col("precio_lista"))
           .withColumn("monto_bruto", F.round(F.col("cantidad") * F.col("precio_unitario"), 2))
           .withColumn("monto_descuento", F.round(F.col("monto_bruto") * F.col("descuento"), 2))
           .withColumn("monto_neto", F.round(F.col("monto_bruto") - F.col("monto_descuento"), 2))
           .withColumn("costo_total", F.round(F.col("cantidad") * F.col("costo_unitario"), 2))
           .withColumn("utilidad", F.round(F.col("monto_neto") - F.col("costo_total"), 2))
           .withColumn("margen_pct", F.when(F.col("monto_neto") > 0,
                                            F.round(F.col("utilidad") / F.col("monto_neto") * 100, 2)))
           # logística
           .withColumn("dias_envio", F.datediff("fecha_envio", "fecha_orden"))
           .withColumn("estado_envio",
                       F.when(F.col("modo_envio") == "Recojo en tienda", "Recojo")
                        .when(F.col("dias_envio") <= 2, "A tiempo")
                        .when(F.col("dias_envio") <= 5, "Normal")
                        .otherwise("Retrasado"))
           .withColumn("fecha_proceso", F.current_timestamp()))

df_fact = alinear_a_tabla(df_fact, f"{snk}.fact_ventas")

# COMMAND ----------

# MAGIC %md ### MERGE incremental (upsert por `orden_id`)
# MAGIC Si el archivo raw trae nuevas órdenes se insertan; si trae órdenes existentes se actualizan.

# COMMAND ----------

tabla_fact = DeltaTable.forName(spark, f"{snk}.fact_ventas")
(tabla_fact.alias("t")
 .merge(df_fact.alias("s"), "t.orden_id = s.orden_id")
 .whenMatchedUpdateAll()
 .whenNotMatchedInsertAll()
 .execute())

metricas = tabla_fact.history(1).select("operation", "operationMetrics").collect()[0]
print(f"Operación: {metricas['operation']} → {metricas['operationMetrics']}")

df_ventas.unpersist()

# COMMAND ----------

for t in ["dim_clientes", "dim_productos", "fact_ventas"]:
    validar_no_vacia(f"{snk}.{t}")
