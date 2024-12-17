WITH stg_empleados AS (
    SELECT 
        DISTINCT 
        ROW_NUMBER() OVER (ORDER BY LOWER(nombre_del_colaborador)) AS empleado_id,
        TRIM(
            INITCAP(
                TRANSLATE(LOWER(nombre_del_colaborador), 'áéíóú', 'aeiou')
            )
        ) AS nombre_del_colaborador
    FROM {{ source('raw', 'raw_pago_planillas') }}
    WHERE nombre_del_colaborador IS NOT NULL
)

SELECT 
    CAST(empleado_id AS STRING) AS empleado_id,
    nombre_del_colaborador
FROM stg_empleados;
