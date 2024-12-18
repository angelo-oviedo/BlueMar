-- models/raw/raw_ingresos.sql

WITH raw_ingresos AS (
    SELECT
        -- Normalize cantidad
        CAST(CASE
            WHEN LOWER(cantidad) IN ('no apunte', '', 'n/a') THEN NULL
            ELSE cantidad
        END AS FLOAT) AS cantidad,

        -- Normalize estado_factura
        CASE
            WHEN LOWER(estado_factura) IN ('paga', 'pagada') THEN 'Paga'
            WHEN LOWER(estado_factura) IN ('pendiente', 'pend', 'pendientes') THEN 'Pendiente'
            WHEN LOWER(estado_factura) = 'anulada' THEN 'Anulada'
            ELSE estado_factura
        END AS estado_factura,

        -- Normalize cliente names
        CASE
            WHEN LOWER(cliente) LIKE '%saenz%' THEN 'Saenz'
            WHEN LOWER(cliente) LIKE '%jardines%' THEN 'Jardines'
            WHEN LOWER(cliente) LIKE '%coco beer%' THEN 'Coco Beer'
            WHEN LOWER(cliente) LIKE '%calimardo%' THEN 'Calimardo'
            WHEN LOWER(cliente) LIKE '%maraki%' THEN 'Maraki Family'
            WHEN LOWER(cliente) LIKE '%hjk%' THEN 'HJK Gourmet'
            WHEN LOWER(cliente) LIKE '%adomicilio%' THEN 'Adomicilio'
            ELSE cliente
        END AS cliente,

        -- Normalize concepto
        CASE
            WHEN LOWER(concepto) LIKE '%camaron%' THEN 'Camarones'
            WHEN LOWER(concepto) LIKE '%langosta%' THEN 'Langosta'
            WHEN LOWER(concepto) LIKE '%camarones%' THEN 'Camarones'
            WHEN LOWER(concepto) LIKE '%atun%' THEN 'Atún'
            ELSE concepto
        END AS concepto,

        -- Normalize mes
        CASE
            WHEN LOWER(mes) = 'enero' THEN 'Enero'
            WHEN LOWER(mes) = 'febrero' THEN 'Febrero'
            WHEN LOWER(mes) = 'marzo' THEN 'Marzo'
            WHEN LOWER(mes) = 'abril' THEN 'Abril'
            WHEN LOWER(mes) = 'mayo' THEN 'Mayo'
            WHEN LOWER(mes) = 'junio' THEN 'Junio'
            WHEN LOWER(mes) = 'julio' THEN 'Julio'
            WHEN LOWER(mes) = 'agosto' THEN 'Agosto'
            WHEN LOWER(mes) = 'septiembre' THEN 'Septiembre'
            WHEN LOWER(mes) = 'octubre' THEN 'Octubre'
            WHEN LOWER(mes) = 'noviembre' THEN 'Noviembre'
            WHEN LOWER(mes) = 'diciembre' THEN 'Diciembre'
            ELSE mes
        END AS mes,

        -- Normalize talla
        CASE
            WHEN LOWER(talla) LIKE '%cm%' THEN CONCAT(CAST(REGEXP_EXTRACT(talla, r'(\d+\.?\d*)') AS FLOAT), 'cm')
            ELSE talla
        END AS talla,

        -- Normalize referencia_banco
        CASE
            WHEN LOWER(referencia_banco) LIKE '%bac%' THEN 'BAC'
            WHEN LOWER(referencia_banco) LIKE '%bn%' THEN 'Banco Nacional'
            WHEN LOWER(referencia_banco) = 'sin referencia' THEN NULL
            ELSE referencia_banco
        END AS referencia_banco,

        -- Parse and cast dates
        TO_DATE(fecha, 'DD/MM/YYYY') AS fecha,

        -- Cast numeric columns
        CAST(precio_unitario AS FLOAT) AS precio_unitario,
        CAST(total AS FLOAT) AS total,

        -- Include unchanged columns
        factura
    FROM {{ source('raw', 'processed_ingresos') }}
)

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
FROM raw_ingresos