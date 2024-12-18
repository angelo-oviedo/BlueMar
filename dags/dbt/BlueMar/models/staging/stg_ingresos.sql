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
=======
        -- Generate a unique ingreso_id
        md5(concat_ws(
            '|',
            COALESCE(cliente, 'N/A'),
            COALESCE(REGEXP_REPLACE(factura, '[^0-9]', ''), '0'),  -- Clean factura
            COALESCE(fecha, '1900-01-01'),
            COALESCE(referencia_banco, 'N/A')
        )) AS ingreso_id,

        -- Handle empty strings and nulls for numeric columns
        CASE
            WHEN TRIM(precio_unitario) = '' OR precio_unitario IS NULL THEN 0
            ELSE REPLACE(REPLACE(precio_unitario, '₡', ''), ',', '')::DECIMAL(10,4)
        END AS precio_unitario,

        CASE
            WHEN TRIM(total) = '' OR total IS NULL THEN 0
            ELSE REPLACE(REPLACE(total, '₡', ''), ',', '')::DECIMAL(10,4)
        END AS total,

        CASE
            WHEN TRIM(cantidad) = '' OR cantidad IS NULL THEN 0
            ELSE cantidad::INT
        END AS cantidad,

        -- Handle other columns
        COALESCE(INITCAP(cliente), 'Unknown') AS cliente,
        UPPER(COALESCE(estado_factura, 'UNKNOWN')) AS estado_factura,
        COALESCE(REGEXP_REPLACE(factura, '[^0-9]', ''), '0')::INT AS factura,  -- Clean and cast factura
        COALESCE(talla, 'Unknown') AS talla,
        COALESCE(referencia_banco, 'Unknown') AS referencia_banco,

        -- Handle empty or invalid dates
        CASE 
            WHEN fecha IS NULL OR fecha = '' THEN NULL 
            ELSE TO_DATE(fecha, 'DD/MM/YYYY')
        END AS fecha,

        COALESCE(INITCAP(mes), 'Unknown') AS mes
    FROM {{ source('raw', 'processed_ingresos') }}
),

cleaned_ingresos AS (
    SELECT *
    FROM raw_ingresos
    WHERE estado_factura NOT IN ('NULA', 'ANULADA')  
      AND cliente IS NOT NULL
      AND factura IS NOT NULL
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
