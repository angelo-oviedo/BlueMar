-- models/raw/raw_pago_planillas.sql

with raw_pago_planillas as (
    SELECT
        costo_por_hora,
        fecha_del_pago,
        horas_trabajadas,
        mes,
        nombre_del_colaborador,
        pago_total
    FROM 
        {{source('raw', 'processed_pago_planillas')}}
)

SELECT * FROM raw_pago_planillas