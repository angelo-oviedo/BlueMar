use role bluemar_dev_elt_developer_role;
use database bluemar_dev_db;
use schema raw;
use warehouse bluemar_dev_elt_wh;

select * from BLUEMAR_DEV_DB.BLUEMAR_DEV_DBT_SCHEMA_RAW.RAW_INGRESOS;
drop view BLUEMAR_DEV_DB.BLUEMAR_DEV_DBT_SCHEMA_RAW.raw_ingresos;



select * from bluemar_dev_db.raw.processed_ingresos;

SELECT factura, COUNT(*) AS count
FROM BLUEMAR_DEV_DB.BLUEMAR_DEV_DBT_SCHEMA_RAW.raw_ingresos
GROUP BY factura
ORDER BY count DESC;

select * from BLUEMAR_DEV_DB.BLUEMAR_DEV_DBT_SCHEMA_STAGING.STG_ingresos;
drop table stg_empleados;

describe schema BLUEMAR_DEV_DB.BLUEMAR_DEV_DBT_SCHEMA_RAW;

DROP VIEW IF EXISTS BLUEMAR_DEV_DB.BLUEMAR_DEV_DBT_SCHEMA_RAW.raw_ingresos;
DROP VIEW IF EXISTS BLUEMAR_DEV_DB.BLUEMAR_DEV_DBT_SCHEMA_RAW.raw_gastos_operativos;
DROP VIEW IF EXISTS BLUEMAR_DEV_DB.BLUEMAR_DEV_DBT_SCHEMA_RAW.raw_inventario_de_activos;
DROP VIEW IF EXISTS BLUEMAR_DEV_DB.BLUEMAR_DEV_DBT_SCHEMA_RAW.raw_pago_planillas;

select * from BLUEMAR_DEV_DB.BLUEMAR_DEV_DBT_SCHEMA_STAGING.STG_empleados;


DELETE FROM BLUEMAR_DEV_DB.BLUEMAR_DEV_DBT_SCHEMA_staging.stg_empleados
WHERE LOWER(NOMBRE_DEL_COLABORADOR) = 'andrés';


drop table if exists stg_cliente;

select * from stg_proveedores;
select * from stg_clientes;
select * from stg_empleados;
select * from stg_detalle_gastos;