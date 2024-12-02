{{
    config(
        schema = 'tokens_flare',
        alias = 'erc20',
        post_hook = '{{ expose_spells(\'["flare"]\',
                                    "sector",
                                    "tokens",
                                    \'["hosuke"]\') }}'
    )
}}

SELECT DISTINCT
    'flare' as blockchain,
    contract_address,
    symbol,
    decimals,
    token_name as name
FROM (
    VALUES
    -- Base tokens
    (0x1D80c49BbBCd1C0911346656B529DF9E5c2F783d, 'Wrapped Flare', 'WFLR', 18),
    -- Other tokens
    (0x282b88514A52FcAdCD92b742745398f3574697d4, 'Finu', 'FINU', 18),
    (0x64cB9bE6323c7a2cE39773674F380da30663bae4, 'ScottyCash', 'SCOTTY', 18),
    (0xDE373AE127A11E756A9D1cc7743816928B239283, 'DAX.BEST', 'DAX', 18),
    (0x932E691aA8c8306C4bB0b19F3f00a284371be8Ba, 'Phili Inu', 'PHIL', 18),
    (0x22757fb83836e3F9F0F353126cACD3B1Dc82a387, 'FlareFox', 'FLX', 18),
    (0xa80C114A90565C03BDCAbC1fcF913cC225d2c5ab, 'F-Asset Token', 'FASSET', 18),
    (0xc6B19B06A92B337Cbca5f7334d29d45ec4d5E532, 'Dog on Moon', 'Moon', 18)
) as temp (contract_address, token_name, symbol, decimals)
