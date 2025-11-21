{% macro oneinch_cfg_macro(key) %}


{% set
    config = {
        'project_start_date': '2025-04-10',
    }
%}

{{ return(config[key]) }}


{% endmacro %}