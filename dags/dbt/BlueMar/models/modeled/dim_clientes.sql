-- models/modeled/dim_clientes.sql

SELECT
    cliente_id AS "Cliente ID",  
    nombre AS "Nombre del Cliente"  
FROM {{ ref('stg_clientes') }};
