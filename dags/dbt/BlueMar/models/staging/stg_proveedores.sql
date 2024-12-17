WITH unique_proveedores AS (
    SELECT DISTINCT
        SUBSTRING(MD5(proveedor), 1, 10) AS proveedor_id,
        INITCAP(proveedor) AS nombre
    FROM {{ source('raw', 'raw_gastos_operativos') }}
    WHERE proveedor IS NOT NULL
),

stg_proveedores AS (
    SELECT
        CAST(proveedor_id AS STRING) AS proveedor_id,
        nombre
    FROM unique_proveedores
)

SELECT
    proveedor_id,
    nombre
FROM stg_proveedores;