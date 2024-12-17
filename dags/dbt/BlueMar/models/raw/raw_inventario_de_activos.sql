-- models/raw/raw_inventario_de_activos.sql

WITH raw_inventario_de_activos AS (
    SELECT
        -- Normalize `activo`
        CASE 
            WHEN LOWER(activo) LIKE '%impresora%' THEN 'Impresora'
            WHEN LOWER(activo) LIKE '%hielera%' THEN 'Hielera'
            WHEN LOWER(activo) LIKE '%cuadraciclo%' THEN 'Cuadraciclo'
            WHEN LOWER(activo) LIKE '%congelador%' THEN 'Congelador'
            ELSE activo
        END AS activo,

        -- Cast and clean `ano_de_adquisicion`
        CAST(ano_de_adquisicion AS INT) AS ano_de_adquisicion,

        -- Cast `cantidad`, `costo`, and `total` to numeric types
        CAST(cantidad AS FLOAT) AS cantidad,
        CAST(costo AS DECIMAL(10, 4)) AS costo,
        CAST(total AS DECIMAL(10, 4)) AS total,

        -- Normalize `fuente_de_financiamiento`
        CASE
            WHEN LOWER(fuente_de_financiamiento) LIKE '%efect%' THEN 'Efectivo'
            WHEN LOWER(fuente_de_financiamiento) LIKE '%contado%' THEN 'Contado'
            WHEN LOWER(fuente_de_financiamiento) LIKE '%presupuesto%' AND LOWER(fuente_de_financiamiento) LIKE '%eww%' THEN 'Presupuesto Estatal'
            WHEN LOWER(fuente_de_financiamiento) LIKE '%presupuesto%' THEN 'Presupuesto Anual'
            WHEN LOWER(fuente_de_financiamiento) LIKE '%donación%' OR LOWER(fuente_de_financiamiento) LIKE '%donacion%' THEN 'Donación'
            WHEN LOWER(fuente_de_financiamiento) LIKE '%recursos propios%' THEN 'Recursos Propios'
            WHEN LOWER(fuente_de_financiamiento) = 's' THEN NULL
            ELSE fuente_de_financiamiento
        END AS fuente_de_financiamiento,

        -- Normalize `tipo`
        CASE
            WHEN LOWER(tipo) LIKE '%coleman%' THEN 'Hielera Coleman'
            WHEN LOWER(tipo) LIKE '%industrial%' THEN 'Equipamiento Industrial'
            ELSE tipo
        END AS tipo

    FROM {{ source('raw', 'processed_inventario_de_activos') }}
),
stg_activos AS (
    SELECT
        -- Create a unique ID for each `activo`
        CONCAT(LOWER(TRIM(activo)), '_', CAST(ano_de_adquisicion AS STRING)) AS activo_id,
        TRIM(activo) AS nombre,
        CAST(cantidad AS INT) AS cantidad,
        CAST(costo AS DECIMAL(10, 4)) AS costo,
        CAST(total AS DECIMAL(10, 4)) AS total,
        ano_de_adquisicion,
        TRIM(tipo) AS tipo,
        TRIM(fuente_de_financiamiento) AS fuente_de_financiamiento
    FROM raw_inventario_de_activos
)

SELECT
    activo_id,
    nombre,
    cantidad,
    costo,
    total,
    ano_de_adquisicion,
    tipo,
    fuente_de_financiamiento
FROM stg_activos;
