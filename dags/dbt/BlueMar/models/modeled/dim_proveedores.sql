-- models/modeled/dim_proveedores.sql

SELECT
    proveedor_id AS "Proveedor ID",
    nombre AS "Nombre del Proveedor"
FROM {{ ref('stg_proveedores') }};
