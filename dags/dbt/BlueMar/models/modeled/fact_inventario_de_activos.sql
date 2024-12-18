-- models/modeled/fact_inventario_de_activos.sql

SELECT
    inventario_id AS "Inventario ID",
    gasto_id AS "Gasto ID",
    activo_id AS "Activo ID",
    cantidad AS "Cantidad"
FROM {{ ref('stg_inventario_activos') }};
