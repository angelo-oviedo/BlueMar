WITH source_data AS (
    SELECT
        -- Generar un ID único para Planilla_Id
        MD5(
            CONCAT_WS(
                '|',
                COALESCE(se.empleado_id, 'N/A'),
                COALESCE(sg.gasto_operativo_id, 'N/A'),
                TO_CHAR(rp.fecha_del_pago, 'YYYYMMDD')
            )
        ) AS planilla_id,

        -- Claves foráneas
        se.empleado_id,
        sg.gasto_operativo_id,

        -- Otros campos
        TRY_TO_DATE(rp.fecha_del_pago, 'YYYY-MM-DD') AS fecha_del_pago,
        CAST(rp.horas_trabajadas AS DECIMAL(10, 4)) AS horas_trabajadas,
        CAST(rp.costo_por_hora AS DECIMAL(10, 4)) AS costo_por_hora,
        CAST(rp.pago_total AS DECIMAL(10, 4)) AS pago_total,
        rp.mes

    FROM {{ source('raw', 'raw_pago_planillas') }} rp

    -- Join con la tabla stg_empleados usando empleado_id
    LEFT JOIN {{ ref('stg_empleados') }} se
        ON rp.empleado_id = se.empleado_id

    -- Join con la tabla stg_gastos_operativos usando gasto_operativo_id
    LEFT JOIN {{ ref('stg_gastos_operativos') }} sg
        ON rp.gasto_operativo_id = sg.gasto_operativo_id

    WHERE rp.empleado_id IS NOT NULL
      AND rp.gasto_operativo_id IS NOT NULL
      AND rp.fecha_del_pago IS NOT NULL
)

SELECT
    CAST(planilla_id AS STRING) AS planilla_id,
    empleado_id,
    gasto_operativo_id,
    fecha_del_pago,
    horas_trabajadas,
    costo_por_hora,
    pago_total,
    mes
FROM source_data;
