{{ config(
     schema = 'op_chains'
        , alias = 'chain_list'
        , unique_key = ['blockchain', 'chain_id']
        , post_hook='{{ expose_spells(\'["optimism","base","zora"]\',
                                  "project",
                                  "op_chains",
                                  \'["msilb7"]\') }}'
  )
}}

{% set op_chains = all_op_chains() %} --macro: all_op_chains.sql

WITH chain_names AS (
        {% for chain in op_chains %}
                SELECT chain AS chain_dune_name -- name of the chain's dune database
        {% if not loop.last %}
                UNION ALL
        {% endif %}
        {% endfor %}
)

SELECT 
        blockchain,
        name as blockchain_name,
        chain_id,
        cast(first_block_time AS date) AS start_date,
        1 as is_superchain

FROM chain_names c
        LEFT JOIN {{ ref('evms_info') }} i 
                ON i.blockchain = c.chain_dune_name