-- models/modeled/dim_empleados.sql

SELECT
    empleado_id AS "Empleado ID",  
    nombre_del_colaborador AS "Nombre del Colaborador"  
FROM {{ ref('stg_empleados') }};
