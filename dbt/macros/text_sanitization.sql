{# ESPOO, espoo, Espoo all to Espoo #}
{% macro normalize_noun_case(column) %}
    upper(left(trim({{ column }}), 1)) || lower(substring(trim({{ column }}) from 2))
{% endmacro %}
