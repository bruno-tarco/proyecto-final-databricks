# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Preparación de ambiente
# MAGIC Crea, de forma **idempotente**, todos los objetos de Unity Catalog del proyecto:
# MAGIC 1. External Locations sobre ADLS Gen2 (usando la Storage Credential ligada a la **Managed Identity** del Access Connector).
# MAGIC 2. Catálogo del ambiente (`dev` o `prod`) y esquemas `bronze`, `silver`, `golden`.
# MAGIC 3. Tablas Delta externas de cada capa con su esquema explícito.
# MAGIC
# MAGIC > La capa **raw** es el contenedor `raw` del Data Lake (no DBFS ni Volúmenes).
# MAGIC > Si `reiniciar = true` se elimina el catálogo y las rutas físicas antes de recrear todo.

# COMMAND ----------

# MAGIC %run ./00_utilidades

# COMMAND ----------

dbutils.widgets.text("storageName", "adlsretaildevbt2026")
dbutils.widgets.text("catalogo", "catalog_retail_dev")
dbutils.widgets.text("credencial", "cred_retail_dev")
dbutils.widgets.dropdown("reiniciar", "false", ["false", "true"])

storage = dbutils.widgets.get("storageName")
catalogo = dbutils.widgets.get("catalogo")
credencial = dbutils.widgets.get("credencial")
reiniciar = dbutils.widgets.get("reiniciar") == "true"

CAPAS = ["bronze", "silver", "golden"]
print(f"Storage: {storage} | Catálogo: {catalogo} | Credencial: {credencial} | Reiniciar: {reiniciar}")

# COMMAND ----------

# MAGIC %md ## 1. External Locations (Managed Identity)

# COMMAND ----------

for contenedor in ["raw", "metastore"] + CAPAS:
    nombre = f"exlt_{storage}_{contenedor}"
    spark.sql(f"""
        CREATE EXTERNAL LOCATION IF NOT EXISTS `{nombre}`
        URL '{ruta_abfss(contenedor, storage)}'
        WITH (STORAGE CREDENTIAL `{credencial}`)
        COMMENT 'Contenedor {contenedor} del Data Lake - proyecto Retail Medallion'
    """)
    print(f"External location lista: {nombre}")

# COMMAND ----------

# MAGIC %md ## 2. Reinicio opcional (catálogo + rutas físicas)

# COMMAND ----------

if reiniciar:
    spark.sql(f"DROP CATALOG IF EXISTS {catalogo} CASCADE")
    for capa in CAPAS:
        dbutils.fs.rm(ruta_abfss(capa, storage, catalogo), True)
    print("Catálogo y rutas físicas eliminados")

# COMMAND ----------

# MAGIC %md ## 3. Catálogo y esquemas

# COMMAND ----------

spark.sql(f"""
    CREATE CATALOG IF NOT EXISTS {catalogo}
    MANAGED LOCATION '{ruta_abfss("metastore", storage, catalogo)}'
    COMMENT 'Catálogo de la arquitectura medallion - Retail'
""")
for capa in CAPAS:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalogo}.{capa} COMMENT 'Capa {capa}'")
display(spark.sql(f"SHOW SCHEMAS IN {catalogo}"))

# COMMAND ----------

# MAGIC %md ## 4. Tablas Delta externas

# COMMAND ----------

def crear_tabla(capa: str, tabla: str, columnas: str, particion: str = None, comentario: str = ""):
    particion_sql = f"PARTITIONED BY ({particion})" if particion else ""
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalogo}.{capa}.{tabla} (
            {columnas}
        )
        USING DELTA
        {particion_sql}
        LOCATION '{ruta_abfss(capa, storage, f"{catalogo}/{tabla}")}'
        COMMENT '{comentario}'
    """)
    print(f"Tabla lista: {catalogo}.{capa}.{tabla}")

# COMMAND ----------

# ---------------- BRONZE: datos tal cual llegan + metadatos de ingesta ----------------
crear_tabla("bronze", "clientes", """
    cliente_id STRING, nombre STRING, segmento STRING, ciudad STRING, region STRING,
    fecha_registro STRING, email STRING, archivo_origen STRING, fecha_ingesta TIMESTAMP
""", comentario="Clientes crudos desde raw/clientes.csv")

crear_tabla("bronze", "productos", """
    producto_id STRING, nombre_producto STRING, categoria STRING, subcategoria STRING,
    precio_lista DOUBLE, costo_unitario DOUBLE, proveedor STRING, activo BOOLEAN,
    archivo_origen STRING, fecha_ingesta TIMESTAMP
""", comentario="Productos crudos desde raw/productos.json")

crear_tabla("bronze", "ventas", """
    orden_id STRING, fecha_orden STRING, fecha_envio STRING, cliente_id STRING, producto_id STRING,
    cantidad INT, descuento DOUBLE, modo_envio STRING, canal STRING,
    archivo_origen STRING, fecha_ingesta TIMESTAMP
""", comentario="Ventas crudas desde raw/ventas.csv")

# COMMAND ----------

# ---------------- SILVER: datos limpios, tipados y enriquecidos ----------------
crear_tabla("silver", "dim_clientes", """
    cliente_id STRING, nombre STRING, segmento STRING, ciudad STRING, region STRING,
    fecha_registro DATE, email STRING, email_enmascarado STRING, email_valido BOOLEAN,
    antiguedad_dias INT, fecha_proceso TIMESTAMP
""", comentario="Dimensión clientes depurada")

crear_tabla("silver", "dim_productos", """
    producto_id STRING, nombre_producto STRING, categoria STRING, subcategoria STRING, proveedor STRING,
    precio_lista DOUBLE, costo_unitario DOUBLE, margen_unitario_pct DOUBLE, rango_precio STRING,
    activo BOOLEAN, fecha_proceso TIMESTAMP
""", comentario="Dimensión productos depurada")

crear_tabla("silver", "fact_ventas", """
    orden_id STRING, fecha_orden DATE, fecha_envio DATE, anio INT, mes INT, trimestre INT, dia_semana STRING,
    cliente_id STRING, segmento STRING, ciudad STRING, region STRING,
    producto_id STRING, nombre_producto STRING, categoria STRING, subcategoria STRING,
    canal STRING, modo_envio STRING, cantidad INT, precio_unitario DOUBLE, descuento DOUBLE,
    monto_bruto DOUBLE, monto_descuento DOUBLE, monto_neto DOUBLE, costo_total DOUBLE,
    utilidad DOUBLE, margen_pct DOUBLE, dias_envio INT, estado_envio STRING, fecha_proceso TIMESTAMP
""", particion="anio", comentario="Hechos de ventas validados y enriquecidos (MERGE incremental)")

crear_tabla("silver", "ventas_rechazadas", """
    orden_id STRING, fecha_orden STRING, cliente_id STRING, producto_id STRING, cantidad INT,
    motivo_rechazo STRING, fecha_proceso TIMESTAMP
""", comentario="Cuarentena: registros que no pasaron las reglas de calidad")

# COMMAND ----------

# ---------------- GOLDEN: tablas de negocio para consumo (BI) ----------------
crear_tabla("golden", "ventas_mensuales_categoria", """
    periodo STRING, anio INT, mes INT, categoria STRING, ordenes BIGINT, unidades BIGINT,
    ventas_netas DOUBLE, utilidad DOUBLE, margen_pct DOUBLE, ticket_promedio DOUBLE,
    ventas_mes_anterior DOUBLE, crecimiento_mom_pct DOUBLE, ventas_acumuladas_anio DOUBLE,
    fecha_carga TIMESTAMP
""", comentario="Evolución mensual por categoría con crecimiento MoM y acumulado anual")

crear_tabla("golden", "ranking_clientes", """
    cliente_id STRING, nombre STRING, segmento STRING, region STRING, ciudad STRING,
    ordenes BIGINT, unidades BIGINT, ventas_netas DOUBLE, utilidad DOUBLE, ticket_promedio DOUBLE,
    ultima_compra DATE, dias_sin_comprar INT, ranking_general INT, ranking_region INT,
    cuartil_valor INT, tipo_cliente STRING, fecha_carga TIMESTAMP
""", comentario="Ranking y segmentación de clientes por valor y recencia")

crear_tabla("golden", "desempeno_productos", """
    producto_id STRING, nombre_producto STRING, categoria STRING, subcategoria STRING,
    unidades BIGINT, ventas_netas DOUBLE, utilidad DOUBLE, margen_pct DOUBLE,
    participacion_categoria_pct DOUBLE, ranking_categoria INT, fecha_carga TIMESTAMP
""", comentario="Desempeño y participación de cada producto dentro de su categoría")

crear_tabla("golden", "kpi_region_canal", """
    region STRING, canal STRING, ordenes BIGINT, clientes_unicos BIGINT, ventas_netas DOUBLE,
    utilidad DOUBLE, dias_envio_promedio DOUBLE, pct_envio_a_tiempo DOUBLE, fecha_carga TIMESTAMP
""", comentario="KPIs comerciales y logísticos por región y canal")

# COMMAND ----------

for capa in CAPAS:
    display(spark.sql(f"SHOW TABLES IN {catalogo}.{capa}"))
