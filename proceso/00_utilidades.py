# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Utilidades comunes
# MAGIC Funciones reutilizadas por todos los notebooks del ETL (se importan con `%run ./00_utilidades`).
# MAGIC - Construcción de rutas `abfss://` hacia ADLS Gen2 (acceso vía **Managed Identity** del Access Connector).
# MAGIC - Escritura tipada en tablas Delta registradas en Unity Catalog.
# MAGIC - Parseo seguro de fechas y validaciones de calidad.

# COMMAND ----------

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import *

REGEX_FECHA = r"^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$"
REGEX_EMAIL = r"^[^@\s]+@[^@\s]+\.[a-z]{2,}$"


def ruta_abfss(contenedor: str, storage: str, subruta: str = "") -> str:
    """Ruta física en ADLS Gen2. La autenticación la resuelve Unity Catalog
    a través de la External Location + Storage Credential (Managed Identity)."""
    return f"abfss://{contenedor}@{storage}.dfs.core.windows.net/{subruta}"


def parsear_fecha(columna: str):
    """Convierte 'yyyy-MM-dd' a date. Si el texto no es una fecha válida devuelve NULL
    (evita errores con ANSI mode activado en Databricks)."""
    return F.when(F.trim(F.col(columna)).rlike(REGEX_FECHA),
                  F.to_date(F.trim(F.col(columna)), "yyyy-MM-dd"))


def alinear_a_tabla(df: DataFrame, tabla: str) -> DataFrame:
    """Selecciona y castea las columnas del DataFrame en el orden/tipo de la tabla destino."""
    esquema = spark.table(tabla).schema
    faltantes = [c.name for c in esquema.fields if c.name not in df.columns]
    if faltantes:
        raise ValueError(f"Columnas faltantes para {tabla}: {faltantes}")
    return df.select([F.col(c.name).cast(c.dataType).alias(c.name) for c in esquema.fields])


def escribir_tabla(df: DataFrame, tabla: str) -> int:
    """Sobrescribe una tabla Delta existente respetando su esquema (DDL creado en Preparación de Ambiente)."""
    df_final = alinear_a_tabla(df, tabla)
    df_final.write.insertInto(tabla, overwrite=True)
    filas = spark.table(tabla).count()
    print(f"{tabla}: {filas:,} filas escritas")
    return filas


def validar_no_vacia(tabla: str, minimo: int = 1) -> None:
    """Control de calidad: detiene el workflow si una tabla queda vacía."""
    n = spark.table(tabla).count()
    if n < minimo:
        raise Exception(f"Control de calidad: {tabla} tiene {n} filas (mínimo esperado {minimo})")
    print(f"Calidad OK · {tabla}: {n:,} filas")
