{{ config(
    tags=['legacy'],
    schema = 'chainlink_ethereum',
    alias = alias('ccip_onramps', legacy_model=True)
    )
}}

-- DUMMY TABLE, WILL BE REMOVED SOON
SELECT
    1