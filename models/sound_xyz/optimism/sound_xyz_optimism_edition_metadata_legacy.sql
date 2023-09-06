{{
    config(
        tags = ['legacy']
        , alias= alias('edition_metadata', legacy_model=True)
    )
}}

SELECT
    '0x' as nft_contract_address,
    '0x' as edition_address