{{ 
    safe_config(
        blockchain = 'avalanche_c',
        alias_name = 'safes'
    )
}}

{{ 
    safe_safes_creation(
        blockchain = 'avalanche_c',
        project_start_date = '2021-10-05',
        date_filter = true
    ) 
}}