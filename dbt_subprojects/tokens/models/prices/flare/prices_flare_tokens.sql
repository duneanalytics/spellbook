{% set blockchain = 'flare' %}

{{ config(
    schema = 'prices_' + blockchain,
    alias = 'tokens',
    tags = ['static']
    )
}}

SELECT
    token_id
    , '{{ blockchain }}' as blockchain
    , symbol
    , contract_address
    , decimals
FROM
(
    VALUES
    ('flare-flare', 'WFLR', 0x1D80c49BbBCd1C0911346656B529DF9E5c2F783d, 18)
    -- Note: Currently only WFLR has a reliable price feed. Other tokens will be added
    -- when they have established price feeds on major price aggregators.
) as temp (token_id, symbol, contract_address, decimals)
