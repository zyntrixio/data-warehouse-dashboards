{# 
Created by:         Aidan Summerville
Created date:       2022-02-07
Last modified by:   
Last modified date: 

Description:
    This macro supersedes the inbuilt function of the same name.
    It ensures that we output objects to the correct schema as per Lumilinks development standards.

Parameters:
    None

Returns:
    Defined schema name.

Usage:
    Automatic.
#}

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}

        {{ default_schema }}

    {%- else -%}

        {{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}
