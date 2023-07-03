{{ config(
        tags = ['dunesql', 'static'],
        alias = alias('info', timestamp ),
        post_hook='{{ expose_spells(\'["goerli"]\',
                                    "sector",
                                    "evms",
                                    \'["hildobby", "msilb7", "msilb7]\') }}')
}}

SELECT chain_id, blockchain, name, chain_type, rollup_type, native_token_symbol, wrapped_native_token_address, explorer_link, first_block_time
FROM (VALUES
          (1, 'goerli', 'Ethereum Goerli', 'Layer 1', NULL, 'ETH', 0xb4fbf271143f4fbf7b91a5ded31805e42b2208d6, 'https://goerli.etherscan.io', timestamp '2019-01-31 15:10')

        ) AS temp_table (chain_id, blockchain, name, chain_type, rollup_type, native_token_symbol, wrapped_native_token_address, explorer_link, first_block_time)