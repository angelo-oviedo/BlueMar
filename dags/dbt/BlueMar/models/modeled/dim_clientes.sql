-- models/modeled/dim_clientes.sql

SELECT
    cliente_id AS "Cliente ID",  -- Renombrar a un formato más user-friendly
    nombre AS "Nombre del Cliente"  -- Renombrar a un formato más user-friendly
FROM {{ ref('stg_clientes') }};
