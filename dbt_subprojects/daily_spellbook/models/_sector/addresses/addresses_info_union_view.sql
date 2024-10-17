{{ config(
        schema = 'addresses',
        alias ='info_union_view',
        materialized = 'view',
        unique_key = ['address'],
        post_hook='{{ expose_spells(\'["ethereum", "bnb", "avalanche_c", "gnosis", "optimism", "arbitrum", "polygon", "base", "celo", "scroll", "zora", "blast", "fantom", "linea", "zkevm", "zksync"]\',
                                    "sector",
                                    "addresses",
                                    \'["hildobby"]\') }}'
)
}}

{% set addresses_models = [
    ('arbitrum', ref('addresses_arbitrum_info'))
    , ('avalanche_c', ref('addresses_avalanche_c_info'))
    , ('base', ref('addresses_base_info'))
    , ('blast', ref('addresses_blast_info'))
    , ('bnb', ref('addresses_bnb_info'))
    , ('celo', ref('addresses_celo_info'))
    , ('ethereum', ref('addresses_ethereum_info'))
    , ('gnosis', ref('addresses_gnosis_info'))
    , ('optimism', ref('addresses_optimism_info'))
    , ('polygon', ref('addresses_polygon_info'))
    , ('scroll', ref('addresses_scroll_info'))
    , ('zora', ref('addresses_zora_info'))
    , ('fantom', ref('addresses_fantom_info'))
    , ('linea', ref('addresses_linea_info'))
    , ('zkevm', ref('addresses_zkevm_info'))
    , ('zksync', ref('addresses_zksync_info'))
] %}

{% for addresses_model in addresses_models %}
SELECT '{{ addresses_model[0] }}' AS blockchain
, address
, executed_tx_count
, max_nonce
, is_smart_contract
, namespace
, name
, first_funded_by
, first_funded_by_block_time
, tokens_received_count
, tokens_received_tx_count
, tokens_sent_count
, tokens_sent_tx_count
, first_transfer_block_time
, last_transfer_block_time
, first_received_block_number
, last_received_block_number
, first_sent_block_number
, last_sent_block_number
, received_volume_usd
, sent_volume_usd
, first_tx_block_time
, last_tx_block_time
, first_tx_block_number
, last_tx_block_number
--, map_from_entries(array[
--    ('last_seen', CAST(last_seen AS varchar))
--    , ('last_seen_block', CAST(last_seen_block AS varchar))
--    , ('executed_tx_count', CAST(executed_tx_count AS varchar))
--    , ('is_smart_contract', CAST(is_smart_contract AS varchar))
--    , ('tokens_sent_count', CAST(tokens_sent_count AS varchar))
--    , ('tokens_received_count', CAST(tokens_received_count AS varchar))
--    ]) AS chain_stats
, last_seen
, last_seen_block
FROM (select * from {{ addresses_model[1] }}
ORDER BY address asc limit 1000000)
{% if not loop.last %}
UNION ALL
{% endif %}
{% endfor %}
