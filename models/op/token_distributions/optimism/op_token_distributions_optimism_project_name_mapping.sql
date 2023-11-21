-- Map governance proposal names to Dune project names
{{ config(
    tags = ['static'],
    alias = 'project_name_mapping'
    )
}}

WITH unified_mappings AS (
        SELECT proposal_name,
               project_name,
               ROW_NUMBER() OVER (PARTITION BY lower(proposal_name) ORDER BY project_name ASC) AS rn
        FROM (
                SELECT proposal_name, project_name

                FROM (values
                        --set up a query to check that project_names here have a match in Dune project names & labels (some may not exist, like Karma)
                        ('xToken Terminal / Gamma Strategies', 'xToken')
                        ,('Rainbow Wallet', 'Rainbow')
                        ,('Karma 1', 'Karma')
                        ,('Karma 2', 'Karma')
                        ,('Safe', 'Gnosis Safe')
                        ,('Okex', 'OKX')
                        ,('Overtime Markets', 'Thales')
                        ,('Quests on Coinbase Wallet - Quest #1 DEX Swap','Quests on Coinbase Wallet')
                        ,('Quests on Coinbase Wallet - Quest #2 Delegation','Quests on Coinbase Wallet')
                        ,('Uniswap V3','Uniswap')
                        ,('SushiSwap', 'Sushi')
                        ,('Karma delegate registry', 'Karma')
                        ,('Rabbit Hole','Rabbithole')
                        ,('Lyra','Lyra Finance')
                        
                ) a (proposal_name, project_name)

                -- Commenting out for runtime, but future versions should move these refs later
                -- UNION ALL

                -- SELECT
                -- dune_name AS proposal_name, mapped_name AS project_name
                -- FROM contracts_project_name_mappings

        ) u
)

SELECT proposal_name, project_name
FROM unified_mappings
WHERE rn = 1 --ensure no dupes