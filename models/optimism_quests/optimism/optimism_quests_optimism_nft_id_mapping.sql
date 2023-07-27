{{config(
	tags=['legacy'],
	
        schema='optimism_quests_optimism',
        alias = alias('nft_id_mapping', legacy_model=True),
        post_hook='{{ expose_spells(\'["optimism"]\',
                                    "project",
                                    "optimism_quests",
                                    \'["msilb7"]\') }}')}}



with quest_nft_ids AS (
    SELECT contract_project, quest_project, cast(nft_id as varchar(4)) as nft_id
    FROM (values
         ('Beethoven X', 'Beethoven X', 6366)
        ,('Clipper','Clipper', 6357)
        ,('Hop Protocol', 'Hop', 6359)
        ,('Kwenta','Kwenta', 6364)
        ,('Lyra','Lyra', 6358)
        ,('Perpetual Protocol','Perpetual Protocol', 6349)
        ,('Pika Protocol','Pika', 6361)
        ,('Polynomial Protocol','Polynomial', 6346)
        ,('PoolTogether','PoolTogether', 6351)
        ,('QiDao','QiDao', 6363)
        ,('Quix','Quix', 6369)
        ,('Rubicon','Rubicon', 6360)
        ,('Stargate Finance','Stargate', 6340)
        ,('Synapse','Synapse', 6347)
        ,('Synthetix','Synthetix', 6362)
        ,('Granary','The Granary', 6367)
        ,('Uniswap','Uniswap', 6343)
        ,('Velodrome','Velodrome', 6344)
        ) a (contract_project, quest_project,nft_id)
)

SELECT * FROM quest_nft_ids