-- models/raw/raw_gastos_operativos.sql

with raw_gastos_operativos as (
    SELECT
        cantidad_,
        concepto_,
        descripcion_,
        estado_factura,
        factura,
        fecha,
        fecha_de_pago,
        mes,
        monto_unitario,
        proveedor_,
        referencia_banco,
        total
    FROM 
        {{source('raw', 'raw_gastos_operativos')}}
)

SELECT * FROM raw_gastos_operativos