{{
    config(
         tags = ['legacy']
        , alias = alias('edition_metadata',legacy_model=True)
        ,materialized = 'incremental'
        ,file_format = 'delta'
        ,incremental_strategy = 'merge'
        ,unique_key = ['edition_address']
        ,post_hook='{{ expose_spells(\'["optimism"]\',
                                    "project",
                                    "mirror",
                                    \'["msilb7"]\') }}'
    )
}}

SELECT 1