

SELECT address, project, project_type
FROM (VALUES
    -- Source: https://etherscan.io/directory/Exchanges/DEX -- to do: includes all entries from source 
    ('0xc36442b4a4522e871399cd717abdd847ab11fe88', 'uniswap_v3', 'dex')
    , ('0x58a3c68e2d3aaf316239c003779f71acb870ee47', 'curvefi', 'dex')
  
  
    -- Source: XXX -- to do: find source for lending protocols
    , ('0x58a3c68e2d3aaf316239c003779f71acb870ee47', 'alpha_homora', 'lending')
  
    ) AS tmp_table (address, project, project_type)
			
