WITH raw_gastos_operativos AS (
    SELECT
        CAST(CASE
            WHEN LOWER(cantidad) = 'no apunte' THEN NULL
            ELSE cantidad
        END AS FLOAT) AS cantidad,
        CASE 
            WHEN LOWER(estado_factura) IN ('paga', 'pagada') THEN 'Paga'
            ELSE estado_factura
        END AS estado_factura,
        CAST(monto_unitario AS FLOAT) AS monto_unitario,
        CAST(total AS FLOAT) AS total,
        CASE
            WHEN LOWER(concepto) LIKE '%ostras%' OR LOWER(concepto) IN ('osttras', 'ostraw') THEN 'Ostras'
            WHEN LOWER(concepto) LIKE '%mariscos%' THEN 'Mariscos'
            WHEN LOWER(concepto) LIKE '%iva%' THEN 'IVA'
            WHEN LOWER(concepto) LIKE '%servicios contables%' THEN 'Servicios Contables'
            WHEN LOWER(concepto) LIKE '%suministros de limpieza%' THEN 'Suministros de Limpieza'
            WHEN LOWER(concepto) LIKE '%suministros%' THEN 'Suministros'
            WHEN LOWER(concepto) LIKE '%teléfono%' THEN 'Teléfono'
            WHEN LOWER(concepto) LIKE '%seguro%' THEN 'Seguro'
            WHEN LOWER(concepto) LIKE '%riesgos del trabajo%' THEN 'Riesgos del Trabajo'
            WHEN LOWER(concepto) LIKE '%poliza vehicular%' THEN 'Poliza Vehicular'
            WHEN LOWER(concepto) LIKE '%parqueo%' THEN 'Parqueo'
            WHEN LOWER(concepto) LIKE '%mantenimiento de vehículo%' THEN 'Mantenimiento de Vehículo'
            WHEN LOWER(concepto) LIKE '%encomiendas%' THEN 'Encomiendas'
            WHEN LOWER(concepto) LIKE '%depreciacion%' THEN 'Depreciación'
            WHEN LOWER(concepto) LIKE '%combustible%' THEN 'Combustible'
            WHEN LOWER(concepto) LIKE '%alimentación%' THEN 'Alimentación'
            WHEN LOWER(concepto) LIKE '%mantenimiento de instalaciones%' THEN 'Mantenimiento de Instalaciones'
            WHEN LOWER(concepto) LIKE '%uniformes%' THEN 'Uniformes'
            WHEN LOWER(concepto) LIKE '%sistema de facturación%' THEN 'Sistema de Facturación'
            WHEN LOWER(concepto) LIKE '%depuración%' OR LOWER(concepto) IN (
                'depuacion de agosto', 
                'depuracion de ostras', 
                'depuracion', 
                'depuracion', 
                'depuración', 
                'depuracion', 
                'DEpuracion', 
                'DEPURACION', 
                'Depuración', 
                'Depuacion'
            ) THEN 'Depuración'
            WHEN LOWER(concepto) IN ('agosto', 'enero') THEN UPPER(concepto)
            ELSE concepto
        END AS concepto,
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
        CASE
            WHEN LOWER(descripcion) LIKE '%ostras%' THEN 'Ostras'
            WHEN LOWER(descripcion) LIKE '%cm%' OR descripcion IN ('7', '65', '99457') THEN 'Medida en cm'
            WHEN LOWER(descripcion) LIKE '%depuración%' THEN 'Depuración'
            WHEN LOWER(descripcion) LIKE '%mantenimiento%' OR LOWER(descripcion) LIKE '%matenimiento%' THEN 'Mantenimiento'
            WHEN LOWER(descripcion) LIKE '%combustible%' THEN 'Combustible'
            WHEN LOWER(descripcion) LIKE '%alimentacion%' THEN 'Alimentación'
            WHEN LOWER(descripcion) LIKE '%servicios contables%' THEN 'Servicios Contables'
            WHEN LOWER(descripcion) LIKE '%suministros de limpieza%' THEN 'Suministros de Limpieza'
            WHEN LOWER(descripcion) LIKE '%suministros%' THEN 'Suministros'
            WHEN LOWER(descripcion) LIKE '%parqueo%' THEN 'Parqueo'
            WHEN LOWER(descripcion) LIKE '%encomiendas%' THEN 'Encomiendas'
            WHEN LOWER(descripcion) LIKE '%telefono%' THEN 'Teléfono'
            WHEN LOWER(descripcion) LIKE '%seguro%' THEN 'Seguro'
            WHEN LOWER(descripcion) LIKE '%riesgos de trabajo%' THEN 'Riesgos de Trabajo'
            WHEN LOWER(descripcion) LIKE '%poliza vehicular%' THEN 'Poliza Vehicular'
            WHEN LOWER(descripcion) LIKE '%depreciacion%' OR LOWER(descripcion) LIKE '%decreciacion%' THEN 'Depreciación'
            WHEN LOWER(descripcion) LIKE '%fundacion de la universidad cr%' THEN 'Fundación de la Universidad CR'
            WHEN LOWER(descripcion) LIKE '%roni%' THEN 'Roni'
            WHEN LOWER(descripcion) LIKE '%una%' THEN 'Una'
            ELSE descripcion
        END AS descripcion,
        CASE
            WHEN LOWER(proveedor) LIKE '%sindicato%' THEN 'Sindicato'
            WHEN LOWER(proveedor) LIKE '%venado%' OR LOWER(proveedor) IN ('venada', 'venano') THEN 'Venado'
            WHEN LOWER(proveedor) LIKE '%chira%' THEN 'Chira'
            WHEN LOWER(proveedor) LIKE '%acuamar%' OR LOWER(proveedor) LIKE '%acua%' THEN 'Acuamar'
            WHEN LOWER(proveedor) LIKE '%cerro gordo%' THEN 'Cerro Gordo'
            WHEN LOWER(proveedor) LIKE '%funda una%' OR LOWER(proveedor) LIKE '%funda%' THEN 'Funda Una'
            WHEN LOWER(proveedor) LIKE '%roni%' OR LOWER(proveedor) LIKE '%rony%' THEN 'Roni'
            ELSE proveedor
        END AS proveedor,
        referencia_banco,
        TO_DATE(fecha, 'DD/MM/YYYY') AS fecha,
        TO_DATE(fecha_de_pago, 'DD/MM/YYYY') AS fecha_de_pago,
        factura
    FROM 
        {{ source('raw', 'processed_gastos_operativos') }}
)

SELECT 
    cantidad,
    estado_factura,
    monto_unitario,
    total,
    concepto,
    mes,
    descripcion,
    proveedor,
    referencia_banco,
    fecha,
    fecha_de_pago,
    factura
FROM raw_gastos_operativos