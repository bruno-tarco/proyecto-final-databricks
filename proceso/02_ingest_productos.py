# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Ingesta (Extract) · Productos → Bronze
# MAGIC Fuente: `raw/productos.json` en ADLS Gen2 (JSON multilínea, nombres en camelCase).
# MAGIC Se lee con esquema explícito y se renombran las columnas a snake_case en español.

# COMMAND ----------

# MAGIC %run ./00_utilidades

# COMMAND ----------

dbutils.widgets.text("storageName", "adlsretaildevbt2026")
dbutils.widgets.text("catalogo", "catalog_retail_dev")
dbutils.widgets.text("container", "raw")
dbutils.widgets.text("archivo", "productos.json")

storage = dbutils.widgets.get("storageName")
catalogo = dbutils.widgets.get("catalogo")
container = dbutils.widgets.get("container")
archivo = dbutils.widgets.get("archivo")

ruta = ruta_abfss(container, storage, archivo)
tabla_destino = f"{catalogo}.bronze.productos"
print(f"Leyendo: {ruta}")

# COMMAND ----------

esquema_productos = StructType([
    StructField("productId", StringType(), False),
    StructField("productName", StringType(), True),
    StructField("category", StringType(), True),
    StructField("subCategory", StringType(), True),
    StructField("listPrice", DoubleType(), True),
    StructField("unitCost", DoubleType(), True),
    StructField("supplier", StringType(), True),
    StructField("active", BooleanType(), True),
])

df_productos = (spark.read
                .schema(esquema_productos)
                .option("multiLine", True)
                .json(ruta))

# COMMAND ----------

df_bronze = (df_productos
             .select(
                 F.col("productId").alias("producto_id"),
                 F.col("productName").alias("nombre_producto"),
                 F.col("category").alias("categoria"),
                 F.col("subCategory").alias("subcategoria"),
                 F.col("listPrice").alias("precio_lista"),
                 F.col("unitCost").alias("costo_unitario"),
                 F.col("supplier").alias("proveedor"),
                 F.col("active").alias("activo"),
                 F.col("_metadata.file_path").alias("archivo_origen"))
             .withColumn("fecha_ingesta", F.current_timestamp()))

display(df_bronze.limit(10))

# COMMAND ----------

escribir_tabla(df_bronze, tabla_destino)
validar_no_vacia(tabla_destino)
