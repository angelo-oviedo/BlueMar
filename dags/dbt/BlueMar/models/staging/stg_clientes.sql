-- models/staging/stg_clientes.sql

WITH raw_clientes AS (
    SELECT DISTINCT
        ROW_NUMBER() OVER (ORDER BY LOWER(cliente)) AS cliente_id, -- Generar ID único
        cliente AS nombre -- Nombre del cliente
    FROM {{ source('raw', 'raw_ingresos') }}
    WHERE cliente IS NOT NULL
)

SELECT
    CAST(cliente_id AS STRING) AS cliente_id, -- Identificador único del cliente
    INITCAP(TRIM(nombre)) AS nombre -- Normalizar el nombre del cliente (formato título)
FROM raw_clientes;
