WITH raw_gastos_operativos AS (
    SELECT
        -- Generate a unique Proveedor_Id
        SUBSTRING(
            md5( concat_ws(
            '|',
            COALESCE(proveedor, 'N/A')
             )),
            1,
            10
        ) AS Proveedor_Id

        -- Handle other columns
        COALESCE(INITCAP(proveedor), 'Unknown') AS Nombre

    FROM {{ source('raw', 'raw_gastos_operativos') }}
),

cleaned_proveedor AS (
    SELECT *
    FROM raw_gastos_operativos
    WHERE estado_factura NOT IN ('NULA', 'ANULADA')
      AND proveedor IS NOT NULL
)

SELECT *
FROM cleaned_proveedor
