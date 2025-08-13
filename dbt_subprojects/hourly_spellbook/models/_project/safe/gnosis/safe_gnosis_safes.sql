{{ 
    safe_config(
        blockchain = 'gnosis',
        alias_name = 'safes'
    )
}}

{{ 
    safe_safes_creation(
        blockchain = 'gnosis',
        project_start_date = '2020-05-21',
        date_filter = true
    ) 
}}