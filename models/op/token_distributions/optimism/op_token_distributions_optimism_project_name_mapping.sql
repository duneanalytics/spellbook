-- Map governance proposal names to Dune project names
{{ config(
    alias = 'project_name_mapping'
    )
}}

SELECT
        proposal_name, project_name

FROM (values
        --set up a query to check that project_names here have a match in Dune project names & labels (some may not exist, like Karma)
         ('xToken Terminal / Gamma', 'xToken')
        ,('Bankless Academy', 'Bankless')
        ,('Rainbow Wallet', 'Rainbow')
        ,('Karma 1', 'Karma')
        ,('Karma 2', 'Karma')
        ,('Safe', 'Gnosis Safe')
        ,('Okex', 'OKX')
        ,('Overtime Markets', 'Thales')
        ,('Quests on Coinbase Wallet - Quest #1 DEX Swap','Quests on Coinbase Wallet')
        ,('Quests on Coinbase Wallet - Quest #2 Delegation','Quests on Coinbase Wallet')
        
) a (proposal_name, project_name)