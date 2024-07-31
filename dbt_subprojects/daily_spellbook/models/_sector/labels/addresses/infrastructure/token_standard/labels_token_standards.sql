{{config(
        alias = 'token_standards',
        post_hook='{{ expose_spells(\'["arbitrum", "avalanche_c", "bnb", "ethereum", "fantom", "gnosis","goerli","optimism","polygon"]\',
                                    "sector",
                                    "labels",
                                    \'["hildobby"]\') }}')}}


{% set labels_models = [
ref('labels_token_standards_arbitrum')
 ,ref('labels_token_standards_avalanche_c')
 ,ref('labels_token_standards_bnb')
 ,ref('labels_token_standards_ethereum')
 ,ref('labels_token_standards_ethereum')
 ,ref('labels_token_standards_fantom')
 ,ref('labels_token_standards_gnosis')
 ,ref('labels_token_standards_goerli')
 ,ref('labels_token_standards_optimism')
 ,ref('labels_token_standards_polygon')
] %}


SELECT *
FROM (
        {% for label in labels_models %}
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
        FROM  {{ label }}
        {% if not loop.last %}
        UNION
        {% endif %}
        {% endfor %}
)
