# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Grants (seguridad en Unity Catalog)
# MAGIC Modelo de permisos por rol (principio de mínimo privilegio):
# MAGIC | Rol | Catálogo | bronze | silver | golden |
# MAGIC |---|---|---|---|---|
# MAGIC | **Analistas** (lectura BI) | USE CATALOG | – | – | USE SCHEMA, SELECT |
# MAGIC | **Ingenieros** (data engineering) | USE CATALOG | USE SCHEMA, SELECT, MODIFY | USE SCHEMA, SELECT, MODIFY, CREATE TABLE | USE SCHEMA, SELECT, MODIFY |
# MAGIC
# MAGIC Los grupos deben existir en la consola de cuenta de Databricks (Account Console → User management → Groups).

# COMMAND ----------

dbutils.widgets.text("catalogo", "catalog_retail_dev")
dbutils.widgets.text("grupo_analistas", "analistas_retail")
dbutils.widgets.text("grupo_ingenieros", "ingenieros_retail")

catalogo = dbutils.widgets.get("catalogo")
analistas = dbutils.widgets.get("grupo_analistas").strip()
ingenieros = dbutils.widgets.get("grupo_ingenieros").strip()

# COMMAND ----------

permisos = [
    # (privilegios, objeto, principal)
    ("USE CATALOG", f"CATALOG {catalogo}", analistas),
    ("USE SCHEMA, SELECT", f"SCHEMA {catalogo}.golden", analistas),

    ("USE CATALOG", f"CATALOG {catalogo}", ingenieros),
    ("USE SCHEMA, SELECT, MODIFY", f"SCHEMA {catalogo}.bronze", ingenieros),
    ("USE SCHEMA, SELECT, MODIFY, CREATE TABLE", f"SCHEMA {catalogo}.silver", ingenieros),
    ("USE SCHEMA, SELECT, MODIFY", f"SCHEMA {catalogo}.golden", ingenieros),
]

errores = []
for privilegios, objeto, principal in permisos:
    if not principal:
        continue
    sentencia = f"GRANT {privilegios} ON {objeto} TO `{principal}`"
    try:
        spark.sql(sentencia)
        print(f"{sentencia}")
    except Exception as e:
        errores.append(sentencia)
        print(f"No se pudo aplicar: {sentencia}\n   {str(e).splitlines()[0]}")

# COMMAND ----------

display(spark.sql(f"SHOW GRANTS ON SCHEMA {catalogo}.golden"))

# COMMAND ----------

if errores:
    print(f"{len(errores)} grants fallaron. Verifica que los grupos existan en la cuenta de Databricks.")
    dbutils.notebook.exit(f"Grants con advertencias: {len(errores)}")
dbutils.notebook.exit("Grants aplicados correctamente")
