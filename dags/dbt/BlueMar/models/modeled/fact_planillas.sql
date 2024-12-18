-- models/modeled/fact_planillas.sql

SELECT
    planilla_id AS "Planilla ID",
    empleado_id AS "Empleado ID",
    gasto_operativo_id AS "Gasto Operativo ID",
    fecha_del_pago AS "Fecha del Pago",
    horas_trabajadas AS "Horas Trabajadas",
    costo_por_hora AS "Costo por Hora",
    pago_total AS "Pago Total",
    mes AS "Mes"
FROM {{ ref('stg_planillas') }};
