{{config(
        
        alias = 'nft',
        post_hook='{{ expose_spells(\'["ethereum","solana"]\',
                                    "sector",
                                    "labels",
                                    \'["soispoke", "NazihKalo"]\') }}'
)}}

{% set nft_labels_models = [
 ref('labels_nft_traders_transactions')
 ,ref('labels_nft_traders_transactions_current')
 ,ref('labels_nft_traders_volume_usd')
 ,ref('labels_nft_traders_volume_usd_current')
 ,ref('labels_nft_users_platforms')
 ,ref('labels_nft_smart_trader_roi_eth')
] %}

SELECT *
FROM (
    {% for model in nft_labels_models %}
    SELECT
          blockchain
         , address
         , name
         , category
         , contributor
         , source
         , created_at
         , updated_at
         , model_name
         , label_type
    FROM {{ model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)