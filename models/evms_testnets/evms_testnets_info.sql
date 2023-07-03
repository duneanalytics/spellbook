{{ config(
        tags = ['dunesql', 'static'],
        alias = alias('info', timestamp ),
        post_hook='{{ expose_spells(\'["goerli"]\',
                                    "sector",
                                    "evms_testnets",
                                    \'["hildobby", "msilb7"]\') }}')
}}

SELECT chain_id, blockchain, name, chain_type, rollup_type, native_token_symbol, wrapped_native_token_address, explorer_link, first_block_time
FROM (VALUES
        (5, 'goerli', 'Ethereum Goerli', 'Layer 1', NULL, 'ETH', 0xb4fbf271143f4fbf7b91a5ded31805e42b2208d6, 'https://goerli.etherscan.io', timestamp '2019-01-31 15:10')
        ,(420, 'op_goerli', 'Optimism Goerli', 'Layer 2', NULL, 'ETH', 0xb4fbf271143f4fbf7b91a5ded31805e42b2208d6, 'https://goerli-optimism.etherscan.io/', timestamp '2022-06-09 02:55')
        ) AS temp_table (chain_id, blockchain, name, chain_type, rollup_type, native_token_symbol, wrapped_native_token_address, explorer_link, first_block_time)