-- models/raw/raw_ingresos.sql

with raw_ingresos as (
        SELECT    
            cantidad,
            cliente,
            estado_factura,
            factura,
            fecha,
            mes,
            precio_unitario,
            referencia_banco,
            talla,
            total
        FROM
            {{source('raw', 'raw_ingresos')}}
    )
    SELECT * FROM raw_ingresos