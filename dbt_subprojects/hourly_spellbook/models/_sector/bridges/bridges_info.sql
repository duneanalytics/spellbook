{{ config(
    schema = 'bridges',
    tags = ['static'],
    alias = 'info',
        post_hook = '{{ expose_spells(\'[
                                        "ethereum"
                                        , "base"
                                        ]\',
                                        "sector",
                                        "bridges",
                                        \'["hildobby"]\') }}')
}}

SELECT bridge_name
    , bridge_version
    , intent_based
    , canonical_bridge
    , token_official_bridge
FROM (
    VALUES
        ('CCTP', '1', true, false, true)
) AS temp_table (bridge_name, bridge_version, intent_based, canonical_bridge, token_official_bridge)
