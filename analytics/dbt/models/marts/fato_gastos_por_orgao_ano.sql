SELECT
    orgao,
    orgao_superior,
    ano,
    SUM(pago) AS total_gasto,
    COUNT(*) AS qtd_pagamentos,
    AVG(pago) AS ticket_medio,
    min(pago) as menor_pagamento,
    max(pago) as maior_pagamento
FROM {{ ref('int_gastos_normalizado') }}
GROUP BY 1, 2, 3