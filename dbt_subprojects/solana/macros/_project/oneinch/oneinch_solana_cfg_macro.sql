{% macro oneinch_solana_cfg_macro(key) %}


{% set
    config = {
        'fusion_start_date': '2025-04-10',
        'cc_start_date': '2025-08-10',
    }
%}

{{ return(config[key]) }}


{% endmacro %}