-- models/modeled/dim_detalle_gastos.sql

SELECT
    gasto_id AS "Gasto ID",  
    proveedor_id AS "Proveedor ID",  
    concepto AS "Concepto",  
    cantidad AS "Cantidad",  
    monto_unitario AS "Monto Unitario",  
    total AS "Total",  
    mes AS "Mes"  
FROM {{ ref('stg_detalle_gastos') }};
