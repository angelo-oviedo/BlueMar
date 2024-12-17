WITH source_data AS (
    SELECT
        MD5(
            CONCAT_WS(
                '|',
                COALESCE(factura, 'N/A'),
                COALESCE(TO_CHAR(fecha, 'YYYYMMDD'), 'N/A')
            )
        ) AS factura_id,

        dc.cliente_id,

        TRY_TO_DATE(fecha, 'YYYY-MM-DD') AS fecha_emision,

        -- Standardize Estado_Factura
        CASE
            WHEN LOWER(estado_factura) IN ('paga', 'pagada') THEN 'Paga'
            WHEN LOWER(estado_factura) IN ('pendiente', 'pend', 'pendientes') THEN 'Pendiente'
            WHEN LOWER(estado_factura) IN ('anulada', 'nula') THEN 'Anulada'
            ELSE estado_factura
        END AS estado_factura,

        -- Cast Total to DECIMAL(10,4)
        CAST(total AS DECIMAL(10, 4)) AS total,

        -- Pass Month as it is
        mes
    FROM {{ source('raw', 'raw_ingresos') }} ri
    LEFT JOIN {{ ref('stg_clientes') }} dc
        ON LOWER(TRIM(ri.cliente)) = LOWER(TRIM(dc.nombre))
    WHERE factura IS NOT NULL
)

SELECT
    CAST(factura_id AS STRING) AS factura_id, 
    cliente_id,
    fecha_emision,
    estado_factura,
    total,
    mes
FROM source_data;
