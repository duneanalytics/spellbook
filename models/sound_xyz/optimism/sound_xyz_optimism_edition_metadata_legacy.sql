{{
    config(
        tags = ['legacy']
        , alias= alias('edition_metadata', legacy_model=True)
    )
}}

SELECT
    1 as nft_contract_address,
    1 as edition_address