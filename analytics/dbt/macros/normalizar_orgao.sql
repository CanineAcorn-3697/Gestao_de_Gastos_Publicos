{% macro normalizar_orgao(coluna) %}
    upper(
        trim(
            regexp_replace({{ coluna }}, '\s+', ' ', 'g')
        )
    )
{% endmacro %}