{{
    config(
        tags = ['legacy'],
        schema = 'nft_celo',
        alias = alias('wash_trades', legacy_model=True)
    )
}}

select 1
