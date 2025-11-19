{# In the data lake, timestamps are stored as timestamp type, without timezone information, so we need to reinterpret them in some cases #}

{% macro datalake_hometime_to_timestamp(column_name) %}
    convert_timezone('{{ var("home_tz") }}', {{ column_name }})
{% endmacro %}

{% macro datalake_hometime_to_timestamptz(column_name) %}
    {{ datalake_hometime_to_timestamp(column_name) }} at time zone '{{ var("home_tz") }}'
{% endmacro %}