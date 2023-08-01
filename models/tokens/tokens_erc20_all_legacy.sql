{{ config(
        alias = alias('erc20_all',legacy_model=True)
        ,tags=['legacy']
)
}}


        SELECT '1' as blockchain, '0x' as contract_address, 'erc20' as standard