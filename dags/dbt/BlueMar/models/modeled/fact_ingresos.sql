-- models/modeled/fact_ingresos.sql

SELECT
    ingreso_id AS "Ingreso ID",
    cliente_id AS "Cliente ID",
    fecha AS "Fecha",
    factura_id AS "Factura ID",
    cantidad AS "Cantidad",
    talla AS "Talla",
    precio_unitario AS "Precio Unitario",
    estado_factura AS "Estado de Factura",
    referencia_banco AS "Referencia Banco",
    total AS "Total",
    mes AS "Mes"
FROM {{ ref('stg_ingresos') }};
