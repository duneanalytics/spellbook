{{ 
    safe_config(
        blockchain = 'avalanche_c',
        alias_name = 'safes'
    )
}}

{{ 
    safe_safes_creation(
        blockchain = 'avalanche_c',
        date_filter = true
    ) 
}}