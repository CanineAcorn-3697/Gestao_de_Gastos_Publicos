select *
from {{ ref('fato_gastos_por_orgao_ano') }}
where total_gasto <= 0