{{ 
    safe_config(
        blockchain = 'zksync',
        alias_name = 'safes'
    )
}}

{{ 
    safe_safes_creation(
        blockchain = 'zksync',
        date_filter = false
    ) 
}}
