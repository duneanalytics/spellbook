{{ 
    safe_config(
        blockchain = 'base',
        alias_name = 'transactions'
    )
}}

{{ 
    safe_transactions(
        blockchain = 'base',
        date_filter = true
    ) 
}}
