-- models/modeled/dim_activos.sql

WITH modeled_activos AS (
    SELECT
        CAST(activo_id AS STRING) AS "Activo ID", -- Identificador único del activo
        nombre AS "Nombre del Activo", -- Nombre descriptivo del activo
        cantidad AS "Cantidad Disponible", -- Cantidad total del activo
        costo AS "Costo Unitario", -- Costo unitario del activo
        total AS "Costo Total", -- Costo total del activo
        anio_de_adquisicion AS "Año de Adquisición", -- Año en que se adquirió el activo
        tipo AS "Tipo de Activo", -- Tipo o categoría del activo
        fuente_de_financiamiento AS "Fuente de Financiamiento" -- Fuente de financiamiento para adquirir el activo
    FROM {{ ref('stg_activos') }}
)

SELECT 
    "Activo ID",
    "Nombre del Activo",
    "Cantidad Disponible",
    "Costo Unitario",
    "Costo Total",
    "Año de Adquisición",
    "Tipo de Activo",
    "Fuente de Financiamiento"
FROM modeled_activos;
