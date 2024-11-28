-- models/raw/raw_inventario_de_activos.sql

with raw_inventario_de_activos as (
    SELECT
        activo,
        ano_de_adquisicion,
        cantidad,
        costo,
        fuente_de_financiamiento,
        tipo,
        total
    FROM 
        {{source('raw', 'processed_inventario_de_activos')}}
)

SELECT * FROM raw_inventario_de_activos