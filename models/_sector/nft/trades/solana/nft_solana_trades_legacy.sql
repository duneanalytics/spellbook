{{
    config(
        schema = 'nft_solana'
        , tags = ['legacy']
        , alias = alias('trades', legacy_model=True)
        ,materialized = 'incremental'
        ,file_format = 'delta'
        ,incremental_strategy = 'merge'
        ,unique_key = ['unique_trade_id','block_slot']
        ,post_hook='{{ expose_spells(\'["solana"]\',
                                    "sector",
                                    "nft",
                                    \'["ilemi"]\') }}'
    )
}}

select 
1 
--dummy model delete later