# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Load · Silver → Golden
# MAGIC Tablas de negocio listas para el dashboard:
# MAGIC - `ventas_mensuales_categoria`: `groupBy/agg` + ventanas `lag` (crecimiento MoM) y suma acumulada anual.
# MAGIC - `ranking_clientes`: `dense_rank`, `ntile` y clasificación por valor + recencia.
# MAGIC - `desempeno_productos`: participación dentro de la categoría (`sum over partition`) y `rank`.
# MAGIC - `kpi_region_canal`: KPIs comerciales y logísticos.

# COMMAND ----------

# MAGIC %run ./00_utilidades

# COMMAND ----------

dbutils.widgets.text("catalogo", "catalog_retail_dev")
dbutils.widgets.text("esquema_source", "silver")
dbutils.widgets.text("esquema_sink", "golden")

catalogo = dbutils.widgets.get("catalogo")
src = f"{catalogo}.{dbutils.widgets.get('esquema_source')}"
snk = f"{catalogo}.{dbutils.widgets.get('esquema_sink')}"

fact = spark.table(f"{src}.fact_ventas")
fact.cache()


def pct(numerador, denominador):
    """División segura (evita división por cero con ANSI mode)."""
    return F.when(F.col(denominador) != 0, F.round(F.col(numerador) / F.col(denominador) * 100, 2))

# COMMAND ----------

# MAGIC %md ## 1. Ventas mensuales por categoría

# COMMAND ----------

w_cat = Window.partitionBy("categoria").orderBy("anio", "mes")
w_acum = (Window.partitionBy("categoria", "anio").orderBy("mes")
          .rowsBetween(Window.unboundedPreceding, Window.currentRow))

df_mensual = (fact.groupBy("anio", "mes", "categoria")
              .agg(F.countDistinct("orden_id").alias("ordenes"),
                   F.sum("cantidad").alias("unidades"),
                   F.round(F.sum("monto_neto"), 2).alias("ventas_netas"),
                   F.round(F.sum("utilidad"), 2).alias("utilidad"))
              .withColumn("periodo", F.format_string("%d-%02d", F.col("anio"), F.col("mes")))
              .withColumn("margen_pct", pct("utilidad", "ventas_netas"))
              .withColumn("ticket_promedio",
                          F.when(F.col("ordenes") > 0, F.round(F.col("ventas_netas") / F.col("ordenes"), 2)))
              .withColumn("ventas_mes_anterior", F.lag("ventas_netas").over(w_cat))
              .withColumn("crecimiento_mom_pct",
                          F.when(F.col("ventas_mes_anterior") > 0,
                                 F.round((F.col("ventas_netas") - F.col("ventas_mes_anterior"))
                                         / F.col("ventas_mes_anterior") * 100, 2)))
              .withColumn("ventas_acumuladas_anio", F.round(F.sum("ventas_netas").over(w_acum), 2))
              .withColumn("fecha_carga", F.current_timestamp()))

escribir_tabla(df_mensual, f"{snk}.ventas_mensuales_categoria")

# COMMAND ----------

# MAGIC %md ## 2. Ranking y segmentación de clientes

# COMMAND ----------

fecha_referencia = fact.agg(F.max("fecha_orden")).collect()[0][0]
print(f"Fecha de referencia para recencia: {fecha_referencia}")

nombres = spark.table(f"{src}.dim_clientes").select("cliente_id", "nombre")

df_clientes = (fact.groupBy("cliente_id", "segmento", "region", "ciudad")
               .agg(F.countDistinct("orden_id").alias("ordenes"),
                    F.sum("cantidad").alias("unidades"),
                    F.round(F.sum("monto_neto"), 2).alias("ventas_netas"),
                    F.round(F.sum("utilidad"), 2).alias("utilidad"),
                    F.max("fecha_orden").alias("ultima_compra"))
               .join(nombres, "cliente_id", "left")
               .withColumn("ticket_promedio", F.round(F.col("ventas_netas") / F.col("ordenes"), 2))
               .withColumn("dias_sin_comprar", F.datediff(F.lit(fecha_referencia), F.col("ultima_compra")))
               .withColumn("ranking_general", F.dense_rank().over(Window.orderBy(F.desc("ventas_netas"))))
               .withColumn("ranking_region",
                           F.dense_rank().over(Window.partitionBy("region").orderBy(F.desc("ventas_netas"))))
               .withColumn("cuartil_valor", F.ntile(4).over(Window.orderBy(F.desc("ventas_netas"))))
               .withColumn("tipo_cliente",
                           F.when((F.col("cuartil_valor") == 1) & (F.col("dias_sin_comprar") <= 90), "VIP activo")
                            .when(F.col("cuartil_valor") == 1, "VIP en riesgo")
                            .when(F.col("dias_sin_comprar") > 180, "Inactivo")
                            .otherwise("Regular"))
               .withColumn("fecha_carga", F.current_timestamp()))

escribir_tabla(df_clientes, f"{snk}.ranking_clientes")

# COMMAND ----------

# MAGIC %md ## 3. Desempeño de productos

# COMMAND ----------

w_categoria = Window.partitionBy("categoria")

df_productos = (fact.groupBy("producto_id", "nombre_producto", "categoria", "subcategoria")
                .agg(F.sum("cantidad").alias("unidades"),
                     F.round(F.sum("monto_neto"), 2).alias("ventas_netas"),
                     F.round(F.sum("utilidad"), 2).alias("utilidad"))
                .withColumn("margen_pct", pct("utilidad", "ventas_netas"))
                .withColumn("ventas_categoria", F.sum("ventas_netas").over(w_categoria))
                .withColumn("participacion_categoria_pct", pct("ventas_netas", "ventas_categoria"))
                .withColumn("ranking_categoria",
                            F.rank().over(w_categoria.orderBy(F.desc("ventas_netas"))))
                .withColumn("fecha_carga", F.current_timestamp()))

escribir_tabla(df_productos, f"{snk}.desempeno_productos")

# COMMAND ----------

# MAGIC %md ## 4. KPIs por región y canal

# COMMAND ----------

es_despacho = F.col("estado_envio") != "Recojo"

df_kpi = (fact.groupBy("region", "canal")
          .agg(F.countDistinct("orden_id").alias("ordenes"),
               F.countDistinct("cliente_id").alias("clientes_unicos"),
               F.round(F.sum("monto_neto"), 2).alias("ventas_netas"),
               F.round(F.sum("utilidad"), 2).alias("utilidad"),
               F.round(F.avg(F.when(es_despacho, F.col("dias_envio"))), 2).alias("dias_envio_promedio"),
               F.round(F.avg(F.when(es_despacho,
                                    F.when(F.col("estado_envio") == "A tiempo", 1).otherwise(0))) * 100, 2)
                .alias("pct_envio_a_tiempo"))
          .withColumn("fecha_carga", F.current_timestamp()))

escribir_tabla(df_kpi, f"{snk}.kpi_region_canal")

# COMMAND ----------

fact.unpersist()
for t in ["ventas_mensuales_categoria", "ranking_clientes", "desempeno_productos", "kpi_region_canal"]:
    validar_no_vacia(f"{snk}.{t}")

display(spark.table(f"{snk}.ranking_clientes").orderBy("ranking_general").limit(10))
