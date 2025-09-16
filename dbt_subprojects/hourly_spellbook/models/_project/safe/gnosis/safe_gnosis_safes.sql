{{ 
    safe_config(
        blockchain = 'gnosis',
        alias_name = 'safes'
    )
}}

{{ 
    safe_safes_creation(
        blockchain = 'gnosis',
        date_filter = false
    ) 
}}