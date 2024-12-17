-- models/raw/raw_pago_planillas.sql

WITH raw_pago_planillas AS (
    SELECT
        -- Cast COSTO_POR_HORA to FLOAT
        CAST(COSTO_POR_HORA AS FLOAT) AS costo_por_hora,

        -- Convert FECHA_DEL_PAGO to DATE
        TO_DATE(FECHA_DEL_PAGO, 'DD/MM/YYYY') AS fecha_del_pago,

        -- Normalize HORAS_TRABAJADAS
        CAST(
            CASE
                WHEN LOWER(HORAS_TRABAJADAS) IN ('no registrado', 'no disponible') THEN NULL
                WHEN LOWER(HORAS_TRABAJADAS) LIKE '%dias%' THEN 
                    CAST(REGEXP_SUBSTR(HORAS_TRABAJADAS, '\\d+') AS FLOAT) * 24
                WHEN REGEXP_LIKE(HORAS_TRABAJADAS, '^[0-9.]+$') THEN CAST(HORAS_TRABAJADAS AS FLOAT)
                ELSE NULL
            END AS FLOAT
        ) AS horas_trabajadas,

        -- Normalize MES
        CASE
            WHEN LOWER(MES) = 'enero' THEN 'Enero'
            WHEN LOWER(MES) = 'febrero' THEN 'Febrero'
            WHEN LOWER(MES) = 'marzo' THEN 'Marzo'
            WHEN LOWER(MES) = 'abril' THEN 'Abril'
            WHEN LOWER(MES) = 'mayo' THEN 'Mayo'
            WHEN LOWER(MES) = 'junio' THEN 'Junio'
            WHEN LOWER(MES) = 'julio' THEN 'Julio'
            WHEN LOWER(MES) = 'agosto' THEN 'Agosto'
            WHEN LOWER(MES) = 'septiembre' THEN 'Septiembre'
            WHEN LOWER(MES) = 'octubre' THEN 'Octubre'
            WHEN LOWER(MES) = 'noviembre' THEN 'Noviembre'
            WHEN LOWER(MES) = 'diciembre' THEN 'Diciembre'
            ELSE MES
        END AS mes,

        -- Normalize NOMBRE_DEL_COLABORADOR
        CASE
            WHEN LOWER(NOMBRE_DEL_COLABORADOR) IN ('no registrado', 'desconocido') THEN 'Colaborador Desconocido'
            WHEN LOWER(NOMBRE_DEL_COLABORADOR) LIKE '%luisa%' THEN 'Luisa'
            WHEN LOWER(NOMBRE_DEL_COLABORADOR) LIKE '%maria%' THEN 'María'
            ELSE NOMBRE_DEL_COLABORADOR
        END AS nombre_del_colaborador,

        -- Cast PAGO_TOTAL to FLOAT
        CAST(PAGO_TOTAL AS FLOAT) AS pago_total
    FROM 
        {{ source('raw', 'processed_pago_planillas') }}
)

SELECT
    costo_por_hora,
    fecha_del_pago,
    horas_trabajadas,
    mes,
    nombre_del_colaborador,
    pago_total
FROM raw_pago_planillas
