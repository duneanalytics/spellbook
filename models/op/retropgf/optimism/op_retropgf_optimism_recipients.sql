{{ config(
        schema = 'op_retropgf_optimism'
        , alias = alias('recipients')
        , post_hook='{{ expose_spells(\'["optimism"]\',
                                  "project",
                                  "op_retropgf",
                                  \'["msilb7"]\') }}'
  )
}}
{% set sources = [
     ('Round #2',   ref('op_retropgf_optimism_round2_recipients'))
] %}

SELECT *
FROM (
    {% for source in sources %}
    SELECT
    'optimism' AS blockchain,
    '{{ source[0] }}' as round_name,
    block_date,
    submitter_address,
    issuer,
    recipient_name,
    recipient_category,
    award_amount,
    award_token

    FROM {{ source[1] }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)