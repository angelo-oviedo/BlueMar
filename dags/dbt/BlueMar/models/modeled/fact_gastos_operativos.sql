-- models/modeled/fact_gastos_operativos.sql

SELECT
    gasto_operativo_id AS "Gasto Operativo ID",   -- Identificador único del gasto operativo
    proveedor_id AS "Proveedor ID",              -- Referencia a DimProveedores
    gasto_id AS "Gasto ID",                      -- Referencia a DimDetalleGastos
    factura_id AS "Factura ID",                  -- Referencia a DimFacturas
    fecha_de_pago AS "Fecha de Pago",            -- Fecha en que se realizó el pago
    estado_factura AS "Estado de Factura",       -- Estado de la factura (normalizado)
    total AS "Total",                            -- Monto total del gasto
    referencia_banco AS "Referencia Banco"       -- Referencia bancaria asociada
FROM {{ ref('stg_gastos_operativos') }};
