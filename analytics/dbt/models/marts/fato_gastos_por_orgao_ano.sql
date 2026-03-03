SELECT
    orgao,
    orgao_superior,
    ano_referencia AS ano,
    SUM(valor_pago) AS total_gasto,
    COUNT(*) AS qtd_pagamentos,
    AVG(valor_pago) AS ticket_medio
FROM {{ ref('int_gastos_normalizado') }}
GROUP BY 1, 2, 3