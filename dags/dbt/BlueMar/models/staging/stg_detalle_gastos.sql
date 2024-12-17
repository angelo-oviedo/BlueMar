WITH stg_detalle_gastos AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY LOWER(concepto)) AS gasto_id, -- Generate a unique ID for each expense
        dp.proveedor_id, -- Reference to DimProveedores
        concepto, -- Expense concept
        CAST(cantidad AS INT) AS cantidad, -- Convert quantity to integer
        CAST(monto_unitario AS DECIMAL(10, 4)) AS monto_unitario, -- Unit amount
        CAST(total AS DECIMAL(10, 4)) AS total, -- Total amount
        mes -- Month
    FROM {{ source('raw', 'raw_gastos_operativos') }} rgo
    LEFT JOIN {{ ref('stg_proveedores') }} dp
        ON LOWER(TRIM(rgo.proveedor)) = LOWER(TRIM(dp.nombre)) -- Join with DimProveedores
)

SELECT
    CAST(gasto_id AS STRING) AS gasto_id,
    proveedor_id,
    concepto,
    cantidad,
    monto_unitario,
    total,
    mes
FROM stg_detalle_gastos;
