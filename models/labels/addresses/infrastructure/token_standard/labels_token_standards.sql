{{config(
	tags=['legacy'],
	alias = alias('token_standards', legacy_model=True),
        post_hook='{{ expose_spells(\'["arbitrum", "avalanche_c", "bnb", "ethereum", "fantom", "gnosis","goerli","optimism","polygon"]\',
                                    "sector",
                                    "labels",
                                    \'["hildobby"]\') }}')}}


{% set labels_models = [
ref('labels_token_standards_arbitrum_legacy')
 ,ref('labels_token_standards_avalanche_c_legacy')
 ,ref('labels_token_standards_bnb_legacy')
 ,ref('labels_token_standards_ethereum_legacy')
 ,ref('labels_token_standards_ethereum_legacy')
 ,ref('labels_token_standards_fantom_legacy')
 ,ref('labels_token_standards_gnosis_legacy')
 ,ref('labels_token_standards_goerli_legacy')
 ,ref('labels_token_standards_optimism_legacy')
 ,ref('labels_token_standards_polygon_legacy')
] %}


SELECT *
FROM (
        {% for label in labels_models %}
        SELECT *
        FROM  {{ label }}
        {% if not loop.last %}
        UNION
        {% endif %}
        {% endfor %}
)
;