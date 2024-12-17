WITH source_data AS (
    SELECT
        -- Generate a unique Ingreso_Id
        MD5(
            CONCAT_WS(
                '|',
                COALESCE(CAST(factura AS STRING), 'N/A'),
                COALESCE(TO_CHAR(fecha, 'YYYYMMDD'), 'N/A')
            )
        ) AS ingreso_id,

        -- Join to fetch Cliente_Id from stg_clientes
        sc.cliente_id,

        -- Join to fetch Factura_Id from stg_facturas
        sf.factura_id,

        -- Normalize and select relevant fields
        TRY_TO_DATE(ri.fecha, 'YYYY-MM-DD') AS fecha,
        CAST(ri.cantidad AS INT) AS cantidad,
        ri.talla,
        CAST(ri.precio_unitario AS DECIMAL(10, 4)) AS precio_unitario,
        ri.estado_factura,
        ri.referencia_banco,
        CAST(ri.total AS DECIMAL(10, 4)) AS total,
        ri.mes
    FROM {{ source('raw', 'raw_ingresos') }} ri
    LEFT JOIN {{ ref('stg_clientes') }} sc
        ON LOWER(TRIM(ri.cliente)) = LOWER(TRIM(sc.nombre))
    LEFT JOIN {{ ref('stg_facturas') }} sf
        ON LOWER(TRIM(ri.factura)) = LOWER(TRIM(sf.factura_id))
)

SELECT
    CAST(ingreso_id AS STRING) AS ingreso_id,
    cliente_id,
    fecha,
    factura_id,
    cantidad,
    talla,
    precio_unitario,
    estado_factura,
    referencia_banco,
    total,
    mes
FROM source_data
WHERE cliente_id IS NOT NULL
  AND factura_id IS NOT NULL;
