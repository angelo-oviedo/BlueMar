WITH fact_gastos_operativos AS (
    SELECT
        CONCAT(
            md5(
                CONCAT_WS(
                    '|', 
                    COALESCE(sp.proveedor_id, 'N/A'),
                    COALESCE(sg.gasto_id, 'N/A'),
                    COALESCE(sf.factura_id, 'N/A')
                )
            ), '_', TO_CHAR(rg.fecha_de_pago, 'YYYYMMDD')
        ) AS gasto_operativo_id,
        sp.proveedor_id,
        sg.gasto_id,
        sf.factura_id,
        TRY_TO_DATE(rg.fecha_de_pago, 'YYYY-MM-DD') AS fecha_de_pago,
        rg.estado_factura,
        CAST(rg.total AS DECIMAL(10, 4)) AS total,
        rg.referencia_banco
    FROM {{ source('raw', 'raw_gastos_operativos') }} rg
    LEFT JOIN {{ ref('stg_proveedores') }} sp
        ON LOWER(TRIM(rg.proveedor)) = LOWER(TRIM(sp.proveedor_id))
    LEFT JOIN {{ ref('stg_detalle_gastos') }} sg
        ON LOWER(TRIM(rg.gasto_id)) = LOWER(TRIM(sg.gasto_id))
    LEFT JOIN {{ ref('stg_facturas') }} sf
        ON LOWER(TRIM(rg.factura)) = LOWER(TRIM(sf.factura_id))
    WHERE rg.estado_factura IS NOT NULL
)

SELECT
    gasto_operativo_id,
    proveedor_id,
    gasto_id,
    factura_id,
    fecha_de_pago,
    estado_factura,
    total,
    referencia_banco
FROM fact_gastos_operativos
