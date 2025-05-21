{{ config(
        alias='native',
        post_hook='{{ expose_spells(\'["arbitrum","avalanche_c","bnb","ethereum","optimism", "gnosis", "fantom", "polygon","solana", "celo", "zksync", "mantle","blast","scroll","linea"]\',
                                    "sector",
                                    "tokens",
                                    \'["0xManny","hildobby","soispoke","dot2dotseurat","mtitus6","wuligy","lgingerich","angus_1","Henrystats","rantum"]\') }}')}}

SELECT
    ei.blockchain AS chain,                      -- From evms.info
    ei.native_token_symbol AS symbol,            -- Native token symbol from evms.info
    erc20.symbol AS price_symbol,                -- WRAPPED token symbol from your ERC20 table
    ei.wrapped_native_token_address AS price_address, -- From evms.info (this is the contract_address for the join)
    erc20.decimals AS decimals                   -- WRAPPED token decimals from your ERC20 table
FROM
    {{ ref('evms_info') }} AS ei
LEFT JOIN
    {{ ref('tokens_erc20') }} AS erc20 -- Assuming your ERC20 table is ref('tokens_erc20')
ON
    ei.wrapped_native_token_address = erc20.contract_address -- Join on the wrapped token address
    AND ei.blockchain = erc20.blockchain -- Crucial: Ensure you also join/filter by blockchain
WHERE
    ei.wrapped_native_token_address IS NOT NULL
    AND ei.is_on_dune
