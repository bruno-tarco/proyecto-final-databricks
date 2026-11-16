# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Exportación Golden → Azure SQL Database (opcional)
# MAGIC Publica las tablas golden en Azure SQL para consumirlas desde Power BI u otras apps.
# MAGIC Las credenciales se leen de un **Secret Scope respaldado por Azure Key Vault** (nunca en el código).
# MAGIC
# MAGIC Secretos esperados en el scope: `sql-server`, `sql-database`, `sql-user`, `sql-password`.
# MAGIC Si `habilitado = false` el notebook termina sin hacer nada (útil si no aprovisionaste Azure SQL).

# COMMAND ----------

dbutils.widgets.text("catalogo", "catalog_retail_dev")
dbutils.widgets.text("secret_scope", "kv-retail")
dbutils.widgets.dropdown("habilitado", "false", ["false", "true"])

catalogo = dbutils.widgets.get("catalogo")
scope = dbutils.widgets.get("secret_scope")

if dbutils.widgets.get("habilitado") != "true":
    dbutils.notebook.exit("Exportación a Azure SQL deshabilitada (habilitado=false)")

# COMMAND ----------

servidor = dbutils.secrets.get(scope, "sql-server")
base_datos = dbutils.secrets.get(scope, "sql-database")
usuario = dbutils.secrets.get(scope, "sql-user")
clave = dbutils.secrets.get(scope, "sql-password")

jdbc_url = (f"jdbc:sqlserver://{servidor}.database.windows.net:1433;database={base_datos};"
            "encrypt=true;trustServerCertificate=false;"
            "hostNameInCertificate=*.database.windows.net;loginTimeout=30;")

# COMMAND ----------

tablas = ["ventas_mensuales_categoria", "ranking_clientes", "desempeno_productos", "kpi_region_canal"]

for tabla in tablas:
    df = spark.table(f"{catalogo}.golden.{tabla}")
    (df.write
       .format("jdbc")
       .option("url", jdbc_url)
       .option("dbtable", f"dbo.{tabla}")
       .option("user", usuario)
       .option("password", clave)
       .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
       .option("truncate", "true")
       .mode("overwrite")
       .save())
    print(f"dbo.{tabla}: {df.count():,} filas exportadas")
