{{ config
(
    tags=['legacy'],
    alias = alias('aggregator_trades', legacy_model=True),
)
}}

-- DUMMY TABLE, WILL BE REMOVED SOON
SELECT 1
