# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Ingesta (Extract) · Clientes → Bronze
# MAGIC Fuente: `raw/clientes.csv` en ADLS Gen2 (CSV con cabecera). Se lee con esquema explícito,
# MAGIC se estandarizan nombres de columnas y se agregan metadatos de ingesta.

# COMMAND ----------

# MAGIC %run ./00_utilidades

# COMMAND ----------

dbutils.widgets.text("storageName", "adlsretaildevbt2026")
dbutils.widgets.text("catalogo", "catalog_retail_dev")
dbutils.widgets.text("container", "raw")
dbutils.widgets.text("archivo", "clientes.csv")

storage = dbutils.widgets.get("storageName")
catalogo = dbutils.widgets.get("catalogo")
container = dbutils.widgets.get("container")
archivo = dbutils.widgets.get("archivo")

ruta = ruta_abfss(container, storage, archivo)
tabla_destino = f"{catalogo}.bronze.clientes"
print(f"Leyendo: {ruta}")

# COMMAND ----------

esquema_clientes = StructType([
    StructField("cliente_id", StringType(), False),
    StructField("nombre", StringType(), True),
    StructField("segmento", StringType(), True),
    StructField("ciudad", StringType(), True),
    StructField("region", StringType(), True),
    StructField("fecha_registro", StringType(), True),  # se tipa en Silver
    StructField("email", StringType(), True),
])

df_clientes = (spark.read
               .option("header", True)
               .option("encoding", "UTF-8")
               .option("mode", "PERMISSIVE")
               .schema(esquema_clientes)
               .csv(ruta))

# COMMAND ----------

df_bronze = (df_clientes
             .withColumn("archivo_origen", F.col("_metadata.file_path"))
             .withColumn("fecha_ingesta", F.current_timestamp()))

display(df_bronze.limit(10))

# COMMAND ----------

escribir_tabla(df_bronze, tabla_destino)
validar_no_vacia(tabla_destino)
