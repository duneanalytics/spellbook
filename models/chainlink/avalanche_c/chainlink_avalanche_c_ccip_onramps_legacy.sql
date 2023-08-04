{{ config(
    tags=['legacy'],
    schema = 'chainlink_avalanche_c',
    alias = alias('ccip_onramps', legacy_model=True)
    )
}}

-- DUMMY TABLE, WILL BE REMOVED SOON
SELECT
    1