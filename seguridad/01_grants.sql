-- =============================================================================
-- SEGURIDAD · GRANTS en Unity Catalog (catálogo de producción)
-- Grupos (crear en Account Console > User management > Groups):
--   analistas_retail  -> solo lectura de la capa golden (consumo BI)
--   ingenieros_retail -> lectura/escritura en bronze, silver y golden
-- =============================================================================

-- ---------- Analistas: mínimo privilegio, solo golden ----------
GRANT USE CATALOG ON CATALOG catalog_retail_prod TO `analistas_retail`;
GRANT USE SCHEMA, SELECT ON SCHEMA catalog_retail_prod.golden TO `analistas_retail`;

-- ---------- Ingenieros de datos ----------
GRANT USE CATALOG ON CATALOG catalog_retail_prod TO `ingenieros_retail`;
GRANT USE SCHEMA, SELECT, MODIFY ON SCHEMA catalog_retail_prod.bronze TO `ingenieros_retail`;
GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA catalog_retail_prod.silver TO `ingenieros_retail`;
GRANT USE SCHEMA, SELECT, MODIFY ON SCHEMA catalog_retail_prod.golden TO `ingenieros_retail`;

-- ---------- Usuario individual (ejemplo: docente revisor) ----------
-- GRANT USE CATALOG ON CATALOG catalog_retail_prod TO `docente@universidad.edu.pe`;
-- GRANT USE SCHEMA, SELECT ON SCHEMA catalog_retail_prod.golden TO `docente@universidad.edu.pe`;

-- ---------- Protección de datos personales ----------
-- Los analistas NO tienen acceso a silver.dim_clientes (contiene email).
-- En golden.ranking_clientes no se expone el email.

-- ---------- Verificación ----------
SHOW GRANTS ON CATALOG catalog_retail_prod;
SHOW GRANTS ON SCHEMA catalog_retail_prod.golden;
SHOW GRANTS `analistas_retail` ON SCHEMA catalog_retail_prod.golden;

-- ---------- Revocar (si fuera necesario) ----------
-- REVOKE SELECT ON SCHEMA catalog_retail_prod.golden FROM `analistas_retail`;
