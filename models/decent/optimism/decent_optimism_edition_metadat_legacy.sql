{{
    config(
        alias = alias('edition_metadata', legacy_model=True)
        ,tags = ['legacy']
        ,materialized = 'incremental'
        ,file_format = 'delta'
        ,incremental_strategy = 'merge'
        ,unique_key = ['nft_contract_address']
        ,post_hook='{{ expose_spells(\'["optimism"]\',
                                    "project",
                                    "decent",
                                    \'["msilb7"]\') }}'
    )
}}

SELECT 1