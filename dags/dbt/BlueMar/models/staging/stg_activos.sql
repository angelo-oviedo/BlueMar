WITH stg_activos AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY LOWER(activo)) AS activo_id,
        activo AS nombre,
        CAST(cantidad AS INT) AS cantidad,
        CAST(costo AS DECIMAL(10, 4)) AS costo,
        CAST(total AS DECIMAL(10, 4)) AS total,
        CAST(anio_de_adquisicion AS INT) AS anio_de_adquisicion,
        tipo,
        fuente_de_financiamiento
    FROM {{ source('raw', 'raw_inventario_de_activos') }}
)

SELECT 
    CAST(activo_id AS STRING) AS activo_id,
    nombre,
    cantidad,
    costo,
    total,
    anio_de_adquisicion AS año_de_adquisicion,
    tipo,
    fuente_de_financiamiento
FROM stg_activos;
