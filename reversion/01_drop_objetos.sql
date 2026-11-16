-- =============================================================================
-- REVERSIÓN · Elimina las tablas lógicas, esquemas y catálogo del proyecto.
-- IMPORTANTE: las tablas son EXTERNAS, por lo que DROP TABLE no borra los
-- archivos en ADLS. Para eliminar las rutas físicas ejecuta reversion/02_reversion.py
-- Reemplaza <catalogo> por catalog_retail_dev o catalog_retail_prod.
-- =============================================================================

-- Delta Sharing
DROP RECIPIENT IF EXISTS rcp_retail_powerbi;
DROP SHARE IF EXISTS share_retail_golden;

-- Golden
DROP TABLE IF EXISTS <catalogo>.golden.ventas_mensuales_categoria;
DROP TABLE IF EXISTS <catalogo>.golden.ranking_clientes;
DROP TABLE IF EXISTS <catalogo>.golden.desempeno_productos;
DROP TABLE IF EXISTS <catalogo>.golden.kpi_region_canal;

-- Silver
DROP TABLE IF EXISTS <catalogo>.silver.fact_ventas;
DROP TABLE IF EXISTS <catalogo>.silver.ventas_rechazadas;
DROP TABLE IF EXISTS <catalogo>.silver.dim_clientes;
DROP TABLE IF EXISTS <catalogo>.silver.dim_productos;

-- Bronze
DROP TABLE IF EXISTS <catalogo>.bronze.ventas;
DROP TABLE IF EXISTS <catalogo>.bronze.productos;
DROP TABLE IF EXISTS <catalogo>.bronze.clientes;

-- Esquemas y catálogo
DROP SCHEMA IF EXISTS <catalogo>.golden CASCADE;
DROP SCHEMA IF EXISTS <catalogo>.silver CASCADE;
DROP SCHEMA IF EXISTS <catalogo>.bronze CASCADE;
DROP CATALOG IF EXISTS <catalogo> CASCADE;

-- External locations (solo si se desmonta todo el ambiente)
-- DROP EXTERNAL LOCATION IF EXISTS `exlt_<storage>_raw`;
-- DROP EXTERNAL LOCATION IF EXISTS `exlt_<storage>_bronze`;
-- DROP EXTERNAL LOCATION IF EXISTS `exlt_<storage>_silver`;
-- DROP EXTERNAL LOCATION IF EXISTS `exlt_<storage>_golden`;
-- DROP EXTERNAL LOCATION IF EXISTS `exlt_<storage>_metastore`;
