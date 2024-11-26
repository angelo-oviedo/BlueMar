WITH calculated_totals AS (
    SELECT
        total,
        cantidad * precio_unitario AS calculated_total
    FROM {{ ref('stg_ingresos') }}
)
SELECT *
FROM calculated_totals
WHERE total != calculated_total
