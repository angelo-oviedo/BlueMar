use role bluemar_dev_elt_developer_role;
use database bluemar_dev_db;
use schema raw;
use warehouse bluemar_dev_elt_wh;

SELECT * FROM raw_gastos_operativos LIMIT 10;
SELECT * FROM raw_ingresos LIMIT 10;
SELECT * FROM raw_pago_planillas lIMIT 10;
SELECT * FROM RAW_INVENTARIO_DE_ACTIVOS LIMIT 10;

SELECT *
FROM BLUEMAR_DEV_DB.BLUEMAR_DEV_DBT_SCHEMA_RAW.RAW_GASTOS_OPERATIVOS
WHERE FACTURA = 'SIN';
