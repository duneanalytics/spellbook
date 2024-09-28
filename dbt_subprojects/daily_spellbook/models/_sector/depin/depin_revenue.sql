{{ config(
     schema = 'depin'
	  , alias = 'revenue'
	  , materialized = 'view'
) }}


{% set models = [
 ref("geodnet_polygon_revenue")
] %}

SELECT * FROM (
{% for model in models %}
    SELECT
        date,
        blockchain,
        project,
        revenue
    FROM {{ model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
{% endfor %}
)
