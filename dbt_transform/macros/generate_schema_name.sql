{% macro generate_schema_name(custom_schema_name=none, node=none) -%}
    {%- set default_schema = target.schema -%}
    
    {%- if custom_schema_name is none -%}
        {%- set final_schema = default_schema -%}
    {%- else -%}
        {%- set final_schema = custom_schema_name | trim -%}
    {%- endif -%}

    {%- set nessie_branch = env_var('NESSIE_BRANCH', 'main') -%}
    
    {%- if nessie_branch != 'main' -%}
        {{ nessie_branch }}.{{ final_schema }}
    {%- else -%}
        {{ final_schema }}
    {%- endif -%}
{%- endmacro %}