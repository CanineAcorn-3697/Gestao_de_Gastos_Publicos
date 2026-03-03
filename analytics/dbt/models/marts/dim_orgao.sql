select
    {{ normalizar_orgao('orgao') }} as orgao,
    orgao_superior,
    count(distinct ano) as anos_com_dados,
    min(ano) as primeiro_ano,
    max(ano) as ultimo_ano,
    sum(pago) as total_historico

from {{ ref('int_gastos_normalizado') }}

group by 1, 2
order by total_historico desc