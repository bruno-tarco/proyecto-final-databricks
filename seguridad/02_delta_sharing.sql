-- =============================================================================
-- SEGURIDAD · DELTA SHARING
-- Comparte las tablas golden con un consumidor externo (p. ej. Power BI de otra
-- organización) sin copiar datos. Requiere ser metastore admin o tener
-- CREATE SHARE / CREATE RECIPIENT en el metastore.
-- =============================================================================

CREATE SHARE IF NOT EXISTS share_retail_golden
  COMMENT 'Tablas golden del proyecto Retail Medallion';

ALTER SHARE share_retail_golden ADD TABLE catalog_retail_prod.golden.ventas_mensuales_categoria;
ALTER SHARE share_retail_golden ADD TABLE catalog_retail_prod.golden.kpi_region_canal;
ALTER SHARE share_retail_golden ADD TABLE catalog_retail_prod.golden.desempeno_productos;

-- Destinatario abierto (genera un enlace de activación con token)
CREATE RECIPIENT IF NOT EXISTS rcp_retail_powerbi
  COMMENT 'Consumidor externo: Power BI';

GRANT SELECT ON SHARE share_retail_golden TO RECIPIENT rcp_retail_powerbi;

-- Obtener el activation link para descargar el archivo de credenciales (.share)
DESCRIBE RECIPIENT rcp_retail_powerbi;
SHOW ALL IN SHARE share_retail_golden;
