-- Consultas que alimentan el dashboard (Databricks AI/BI o Power BI)

-- Ventas mensuales por categoría
SELECT periodo, anio, mes, categoria, ventas_netas, utilidad, margen_pct, crecimiento_mom_pct FROM catalog_retail_prod.golden.ventas_mensuales_categoria;

-- KPIs región y canal
SELECT region, canal, ordenes, clientes_unicos, ventas_netas, utilidad, dias_envio_promedio, pct_envio_a_tiempo FROM catalog_retail_prod.golden.kpi_region_canal;

-- Ranking clientes
SELECT ranking_general, cliente_id, nombre, segmento, region, ordenes, ventas_netas, tipo_cliente, dias_sin_comprar FROM catalog_retail_prod.golden.ranking_clientes;

-- Desempeño productos
SELECT categoria, subcategoria, nombre_producto, ventas_netas, utilidad, margen_pct, participacion_categoria_pct, ranking_categoria FROM catalog_retail_prod.golden.desempeno_productos;
