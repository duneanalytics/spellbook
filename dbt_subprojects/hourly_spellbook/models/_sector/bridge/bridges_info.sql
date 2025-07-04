{{ config(
    schema = 'bridge',
    tags = ['static'],
    alias = 'info',
    post_hook = '{{ expose_spells('["bridge"]', "sector", "bridges", '["yourname"]') }}'
)}}

SELECT project
    , version
    , intent_based
    , canonical_bridge
    , token_official_bridge
FROM (
    VALUES
        ('example_bridge', 'v1', true, true, false)
) AS temp_table (project, version, intent_based, canonical_bridge, token_official_bridge)
