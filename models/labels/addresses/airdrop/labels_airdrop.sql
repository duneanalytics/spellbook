{{config(
        tags=['dunesql', 'prod_exclude'],
        alias = alias('airdrop'),
        post_hook='{{ expose_spells(\'["optimism"]\',
                                    "sector",
                                    "labels",
                                    \'["ilemi", "hosuke"]\') }}'
)}}

{% set airdrop_labels_models = [
 ref('labels_airdrop_1_receivers_optimism')
 ,ref('labels_airdrop_2_receivers_optimism')
 ,ref('labels_airdrop_3_receivers_optimism')
] %}

SELECT *
FROM (
    {% for model in airdrop_labels_models %}
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