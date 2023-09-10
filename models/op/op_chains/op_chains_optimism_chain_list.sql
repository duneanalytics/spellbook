{{ config(
        tags = ['dunesql'],
        schema = 'op_chains',
        alias = alias('chain_list')
        , unique_key = ['blockchain', 'chain_id']
        , post_hook='{{ expose_spells(\'["optimism"]\',
                                  "project",
                                  "op_chains",
                                  \'["msilb7"]\') }}'
  )
}}

SELECT 
        lower(blockchain) AS blockchain,
        blockchain_name,
        chain_id,
        cast(start_date AS date) AS start_date,
        CAST(is_superchain AS double) as is_superchain

FROM(values

         ('optimism',   'Optimism Mainnet', 10, '2021-06-23',   1)
        ,('base',       'Base Mainnet',     8453,   NULL,       1)

) op (blockchain, blockchain_name, chain_id, start_date, is_superchain)