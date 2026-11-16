# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Ingesta (Extract) · Ventas → Bronze
# MAGIC Fuente: `raw/ventas.csv` en ADLS Gen2. Es el insumo transaccional principal (~25k filas).
# MAGIC En Bronze no se descarta nada: las reglas de calidad se aplican en Silver.

# COMMAND ----------

# MAGIC %run ./00_utilidades

# COMMAND ----------

dbutils.widgets.text("storageName", "adlsretaildevbt2026")
dbutils.widgets.text("catalogo", "catalog_retail_dev")
dbutils.widgets.text("container", "raw")
dbutils.widgets.text("archivo", "ventas.csv")

storage = dbutils.widgets.get("storageName")
catalogo = dbutils.widgets.get("catalogo")
container = dbutils.widgets.get("container")
archivo = dbutils.widgets.get("archivo")

ruta = ruta_abfss(container, storage, archivo)
tabla_destino = f"{catalogo}.bronze.ventas"
print(f"Leyendo: {ruta}")

# COMMAND ----------

esquema_ventas = StructType([
    StructField("orden_id", StringType(), False),
    StructField("fecha_orden", StringType(), True),
    StructField("fecha_envio", StringType(), True),
    StructField("cliente_id", StringType(), True),
    StructField("producto_id", StringType(), True),
    StructField("cantidad", IntegerType(), True),
    StructField("descuento", DoubleType(), True),
    StructField("modo_envio", StringType(), True),
    StructField("canal", StringType(), True),
])

df_ventas = (spark.read
             .option("header", True)
             .option("encoding", "UTF-8")
             .schema(esquema_ventas)
             .csv(ruta))

# COMMAND ----------

df_bronze = (df_ventas
             .withColumn("archivo_origen", F.col("_metadata.file_path"))
             .withColumn("fecha_ingesta", F.current_timestamp()))

print(f"Filas leídas: {df_bronze.count():,}")
display(df_bronze.limit(10))

# COMMAND ----------

escribir_tabla(df_bronze, tabla_destino)
validar_no_vacia(tabla_destino, minimo=1000)
