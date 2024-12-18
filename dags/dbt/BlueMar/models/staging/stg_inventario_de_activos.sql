WITH fact_inventario_activos AS (
    SELECT
        -- Generate a unique inventory ID
        CONCAT(
            md5(concat_ws('|', COALESCE(da.activo_id, 'N/A'), COALESCE(dg.gasto_id, 'N/A'))), 
            '_', 
            TO_CHAR(CURRENT_TIMESTAMP, 'YYYYMMDDHH24MISS')
        ) AS inventario_id,
        
        -- Foreign keys
        dg.gasto_id AS gasto_id,
        da.activo_id AS activo_id,

        -- Inventory details
        ra.cantidad AS cantidad
        
    FROM {{ source('raw', 'raw_inventario_activos') }} ra
    -- Join with detalle_gastos for gasto_id
    LEFT JOIN {{ ref('stg_detalle_gastos') }} dg
        ON LOWER(TRIM(ra.gasto)) = LOWER(TRIM(dg.concepto))
    -- Join with activos for activo_id
    LEFT JOIN {{ ref('stg_activos') }} da
        ON LOWER(TRIM(ra.activo)) = LOWER(TRIM(da.nombre))
    WHERE ra.gasto IS NOT NULL AND ra.activo IS NOT NULL
)

SELECT
    inventario_id,
    gasto_id,
    activo_id,
    cantidad
FROM fact_inventario_activos;
