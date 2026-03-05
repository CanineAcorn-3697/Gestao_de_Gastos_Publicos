SELECT
    orgao_superior,
    orgao,
    pago,
    ano
FROM {{ source('silver', 'gastos') }}
WHERE pago > 0