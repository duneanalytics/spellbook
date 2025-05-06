{% macro oneinch_cfg_macro(key) %}


{% set
    config = {
        'project_start_date': '2025-02-22',
        'fusion_program_id': '9CnwB8RDNtRzRcxvkNqwgatRDENBCh2f56HgJLPStn8S',
        'create_discriminator': '0x181ec828051c0777',
        'cancel_discriminator': '0xe8dbdf29dbecdcbe',
        'fill_discriminator': '0xa860b7a35c0a28a0'
    }
%}

{{ return(config[key]) }}


{% endmacro %}