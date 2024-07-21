{# A comment that won't be present in the compiled SQL -#}

-- A comment that will be present

{%- set my_long_variable %}
	SELECT 1 AS my_col
{%- endset %}

{{- my_long_variable -}}

{% set my_list = ['a', 'b', 'c'] %}

SELECT
{%- for item in my_list %}
	{{ item }}{% if not loop.last %},{% endif %}
{%- endfor %}

{{ my_list }}