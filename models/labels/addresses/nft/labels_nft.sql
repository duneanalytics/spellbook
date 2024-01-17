{{config(
        
        alias = 'nft',
        post_hook='{{ expose_spells(\'["ethereum","solana"]\',
                                    "sector",
                                    "labels",
                                    \'["soispoke", "NazihKalo", "kaiblade"]\') }}'
)}}

{% set nft_labels_models = [
 ref('labels_nft_traders_transactions')
 ,ref('labels_nft_traders_transactions_current')
 ,ref('labels_nft_traders_volume_usd')
 ,ref('labels_nft_traders_volume_usd_current')
 ,ref('labels_nft_users_platforms')
 ,ref('labels_nft_smart_trader_roi_eth')
 ,ref('labels_op_nft_minters')
 ,ref('labels_op_nft_traders')
 ,ref('labels_op_habitual_wash_traders')
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