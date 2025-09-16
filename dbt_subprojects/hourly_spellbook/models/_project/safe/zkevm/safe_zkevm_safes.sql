{{ 
    safe_config(
        blockchain = 'zkevm',
        alias_name = 'safes'
    )
}}

{{ 
    safe_safes_creation(
        blockchain = 'zkevm',
        date_filter = false
    ) 
}}