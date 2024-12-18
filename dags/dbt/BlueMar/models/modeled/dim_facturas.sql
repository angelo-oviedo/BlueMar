-- models/modeled/dim_facturas.sql

SELECT
    factura_id AS "Factura ID",           
    cliente_id AS "Cliente ID",           -- Referencia a DimClientes
    fecha_emision AS "Fecha de Emisión",  -- Renombrar a un formato más descriptivo
    estado_factura AS "Estado de Factura",-- Estado de la factura
    total AS "Total",                     -- Monto total de la factura
    mes AS "Mes"                          -- Mes correspondiente
FROM {{ ref('stg_facturas') }};
