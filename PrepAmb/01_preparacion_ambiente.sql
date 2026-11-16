-- =============================================================================
-- PREPARACIÓN DE AMBIENTE · Retail Medallion (versión SQL de referencia)
-- La versión que se ejecuta en el workflow es proceso/01_preparacion_ambiente.py
-- Reemplaza: <storage> = adlsretaildevbt2026 | adlsretailprodbt2026
--            <catalogo> = catalog_retail_dev | catalog_retail_prod
-- Prerrequisito: Storage Credential `cred_retail_<env>` (Azure Managed Identity
-- del Access Connector) creada en Catalog > External data > Credentials.
-- =============================================================================

-- 1. External Locations (acceso a ADLS solo con Managed Identity)
CREATE EXTERNAL LOCATION IF NOT EXISTS `exlt_<storage>_raw`
  URL 'abfss://raw@<storage>.dfs.core.windows.net/' WITH (STORAGE CREDENTIAL `cred_retail_<env>`);
CREATE EXTERNAL LOCATION IF NOT EXISTS `exlt_<storage>_metastore`
  URL 'abfss://metastore@<storage>.dfs.core.windows.net/' WITH (STORAGE CREDENTIAL `cred_retail_<env>`);
CREATE EXTERNAL LOCATION IF NOT EXISTS `exlt_<storage>_bronze`
  URL 'abfss://bronze@<storage>.dfs.core.windows.net/' WITH (STORAGE CREDENTIAL `cred_retail_<env>`);
CREATE EXTERNAL LOCATION IF NOT EXISTS `exlt_<storage>_silver`
  URL 'abfss://silver@<storage>.dfs.core.windows.net/' WITH (STORAGE CREDENTIAL `cred_retail_<env>`);
CREATE EXTERNAL LOCATION IF NOT EXISTS `exlt_<storage>_golden`
  URL 'abfss://golden@<storage>.dfs.core.windows.net/' WITH (STORAGE CREDENTIAL `cred_retail_<env>`);

-- 2. Catálogo y esquemas
CREATE CATALOG IF NOT EXISTS <catalogo>
  MANAGED LOCATION 'abfss://metastore@<storage>.dfs.core.windows.net/<catalogo>';
CREATE SCHEMA IF NOT EXISTS <catalogo>.bronze;
CREATE SCHEMA IF NOT EXISTS <catalogo>.silver;
CREATE SCHEMA IF NOT EXISTS <catalogo>.golden;

-- 3. Tablas BRONZE
CREATE TABLE IF NOT EXISTS <catalogo>.bronze.clientes (
  cliente_id STRING, nombre STRING, segmento STRING, ciudad STRING, region STRING,
  fecha_registro STRING, email STRING, archivo_origen STRING, fecha_ingesta TIMESTAMP
) USING DELTA LOCATION 'abfss://bronze@<storage>.dfs.core.windows.net/<catalogo>/clientes';

CREATE TABLE IF NOT EXISTS <catalogo>.bronze.productos (
  producto_id STRING, nombre_producto STRING, categoria STRING, subcategoria STRING,
  precio_lista DOUBLE, costo_unitario DOUBLE, proveedor STRING, activo BOOLEAN,
  archivo_origen STRING, fecha_ingesta TIMESTAMP
) USING DELTA LOCATION 'abfss://bronze@<storage>.dfs.core.windows.net/<catalogo>/productos';

CREATE TABLE IF NOT EXISTS <catalogo>.bronze.ventas (
  orden_id STRING, fecha_orden STRING, fecha_envio STRING, cliente_id STRING, producto_id STRING,
  cantidad INT, descuento DOUBLE, modo_envio STRING, canal STRING,
  archivo_origen STRING, fecha_ingesta TIMESTAMP
) USING DELTA LOCATION 'abfss://bronze@<storage>.dfs.core.windows.net/<catalogo>/ventas';

-- 4. Tablas SILVER
CREATE TABLE IF NOT EXISTS <catalogo>.silver.dim_clientes (
  cliente_id STRING, nombre STRING, segmento STRING, ciudad STRING, region STRING,
  fecha_registro DATE, email STRING, email_enmascarado STRING, email_valido BOOLEAN,
  antiguedad_dias INT, fecha_proceso TIMESTAMP
) USING DELTA LOCATION 'abfss://silver@<storage>.dfs.core.windows.net/<catalogo>/dim_clientes';

CREATE TABLE IF NOT EXISTS <catalogo>.silver.dim_productos (
  producto_id STRING, nombre_producto STRING, categoria STRING, subcategoria STRING, proveedor STRING,
  precio_lista DOUBLE, costo_unitario DOUBLE, margen_unitario_pct DOUBLE, rango_precio STRING,
  activo BOOLEAN, fecha_proceso TIMESTAMP
) USING DELTA LOCATION 'abfss://silver@<storage>.dfs.core.windows.net/<catalogo>/dim_productos';

CREATE TABLE IF NOT EXISTS <catalogo>.silver.fact_ventas (
  orden_id STRING, fecha_orden DATE, fecha_envio DATE, anio INT, mes INT, trimestre INT, dia_semana STRING,
  cliente_id STRING, segmento STRING, ciudad STRING, region STRING,
  producto_id STRING, nombre_producto STRING, categoria STRING, subcategoria STRING,
  canal STRING, modo_envio STRING, cantidad INT, precio_unitario DOUBLE, descuento DOUBLE,
  monto_bruto DOUBLE, monto_descuento DOUBLE, monto_neto DOUBLE, costo_total DOUBLE,
  utilidad DOUBLE, margen_pct DOUBLE, dias_envio INT, estado_envio STRING, fecha_proceso TIMESTAMP
) USING DELTA PARTITIONED BY (anio)
  LOCATION 'abfss://silver@<storage>.dfs.core.windows.net/<catalogo>/fact_ventas';

CREATE TABLE IF NOT EXISTS <catalogo>.silver.ventas_rechazadas (
  orden_id STRING, fecha_orden STRING, cliente_id STRING, producto_id STRING, cantidad INT,
  motivo_rechazo STRING, fecha_proceso TIMESTAMP
) USING DELTA LOCATION 'abfss://silver@<storage>.dfs.core.windows.net/<catalogo>/ventas_rechazadas';

-- 5. Tablas GOLDEN
CREATE TABLE IF NOT EXISTS <catalogo>.golden.ventas_mensuales_categoria (
  periodo STRING, anio INT, mes INT, categoria STRING, ordenes BIGINT, unidades BIGINT,
  ventas_netas DOUBLE, utilidad DOUBLE, margen_pct DOUBLE, ticket_promedio DOUBLE,
  ventas_mes_anterior DOUBLE, crecimiento_mom_pct DOUBLE, ventas_acumuladas_anio DOUBLE,
  fecha_carga TIMESTAMP
) USING DELTA LOCATION 'abfss://golden@<storage>.dfs.core.windows.net/<catalogo>/ventas_mensuales_categoria';

CREATE TABLE IF NOT EXISTS <catalogo>.golden.ranking_clientes (
  cliente_id STRING, nombre STRING, segmento STRING, region STRING, ciudad STRING,
  ordenes BIGINT, unidades BIGINT, ventas_netas DOUBLE, utilidad DOUBLE, ticket_promedio DOUBLE,
  ultima_compra DATE, dias_sin_comprar INT, ranking_general INT, ranking_region INT,
  cuartil_valor INT, tipo_cliente STRING, fecha_carga TIMESTAMP
) USING DELTA LOCATION 'abfss://golden@<storage>.dfs.core.windows.net/<catalogo>/ranking_clientes';

CREATE TABLE IF NOT EXISTS <catalogo>.golden.desempeno_productos (
  producto_id STRING, nombre_producto STRING, categoria STRING, subcategoria STRING,
  unidades BIGINT, ventas_netas DOUBLE, utilidad DOUBLE, margen_pct DOUBLE,
  participacion_categoria_pct DOUBLE, ranking_categoria INT, fecha_carga TIMESTAMP
) USING DELTA LOCATION 'abfss://golden@<storage>.dfs.core.windows.net/<catalogo>/desempeno_productos';

CREATE TABLE IF NOT EXISTS <catalogo>.golden.kpi_region_canal (
  region STRING, canal STRING, ordenes BIGINT, clientes_unicos BIGINT, ventas_netas DOUBLE,
  utilidad DOUBLE, dias_envio_promedio DOUBLE, pct_envio_a_tiempo DOUBLE, fecha_carga TIMESTAMP
) USING DELTA LOCATION 'abfss://golden@<storage>.dfs.core.windows.net/<catalogo>/kpi_region_canal';
