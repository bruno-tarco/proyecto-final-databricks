# Databricks notebook source
# MAGIC %md
# MAGIC # Reversión completa del ambiente
# MAGIC Elimina **tablas lógicas** (Unity Catalog) y **rutas físicas** (ADLS Gen2) del proyecto.
# MAGIC Irreversible. Escribe `CONFIRMAR` en el widget `confirmacion` para ejecutar.

# COMMAND ----------

dbutils.widgets.text("storageName", "adlsretaildevbt2026")
dbutils.widgets.text("catalogo", "catalog_retail_dev")
dbutils.widgets.text("confirmacion", "")

storage = dbutils.widgets.get("storageName")
catalogo = dbutils.widgets.get("catalogo")

if dbutils.widgets.get("confirmacion") != "CONFIRMAR":
    dbutils.notebook.exit("Reversión cancelada: escribe CONFIRMAR en el widget 'confirmacion'")

# COMMAND ----------

TABLAS = {
    "golden": ["ventas_mensuales_categoria", "ranking_clientes", "desempeno_productos", "kpi_region_canal"],
    "silver": ["fact_ventas", "ventas_rechazadas", "dim_clientes", "dim_productos"],
    "bronze": ["ventas", "productos", "clientes"],
}

# 1. Tablas lógicas
for capa, tablas in TABLAS.items():
    for t in tablas:
        spark.sql(f"DROP TABLE IF EXISTS {catalogo}.{capa}.{t}")
        print(f"Tabla eliminada: {catalogo}.{capa}.{t}")

# 2. Esquemas y catálogo
spark.sql(f"DROP CATALOG IF EXISTS {catalogo} CASCADE")
print(f"Catálogo eliminado: {catalogo}")

# COMMAND ----------

# 3. Rutas físicas en ADLS (las tablas externas no se borran con DROP)
for capa in TABLAS:
    ruta = f"abfss://{capa}@{storage}.dfs.core.windows.net/{catalogo}"
    try:
        dbutils.fs.rm(ruta, True)
        print(f"Ruta física eliminada: {ruta}")
    except Exception as e:
        print(f"{ruta}: {str(e).splitlines()[0]}")

try:
    dbutils.fs.rm(f"abfss://metastore@{storage}.dfs.core.windows.net/{catalogo}", True)
except Exception as e:
    print(f"metastore: {str(e).splitlines()[0]}")
print("Reversión completada")
