{{
    config(
        schema = 'nft_solana'
        , tags = ['legacy']
        , alias = alias('trades', legacy_model=True)
    )
}}

select 
1 
--dummy model delete later