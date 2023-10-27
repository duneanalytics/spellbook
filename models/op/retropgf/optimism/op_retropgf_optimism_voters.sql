{{ config(
        schema = 'op_retropgf_optimism'
        , alias = 'voters'
        
        , post_hook='{{ expose_spells(\'["optimism"]\',
                                  "project",
                                  "op_retropgf",
                                  \'["msilb7"]\') }}'
  )
}}
{% set sources = [
     ('Round #2',   ref('op_retropgf_optimism_round2_voters'))
] %}

SELECT *
FROM (
    {% for source in sources %}
    SELECT
    'optimism' AS blockchain,
    '{{ source[0] }}' as round_name,
    block_date,
    voter,
    issuer,
    can_vote

    FROM {{ source[1] }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)