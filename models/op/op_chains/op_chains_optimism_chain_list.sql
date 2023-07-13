{{ config(
        schema = 'op_chains'
	, tags = ['dunesql', 'static']
        , alias = alias('chain_list')
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
        is_superchain

FROM(values

         ('optimism',	'OP Mainnet',		10,		'2021-06-23',	1)
        ,('zora',	'Zora Network Mainnet',	7777777,	'2023-06-14',	1)
        ,('base',	'Base Mainnet',		8453,		'2023-07-13',	1)

) op (blockchain, blockchain_name, chain_id, start_date, is_superchain)