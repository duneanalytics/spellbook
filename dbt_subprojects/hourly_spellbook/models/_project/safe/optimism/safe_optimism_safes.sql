{{ 
    safe_config(
        blockchain = 'optimism',
        alias_name = 'safes'
    )
}}

{{ 
    safe_safes_creation(
        blockchain = 'optimism',
        date_filter = true
    ) 
}}