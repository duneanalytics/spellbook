{{config(alias='defi',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "sector",
                                    "addresses",
                                    \'["umer_h_adil"]\') }}')}}

SELECT address, project, project_type
FROM (VALUES
	-- dexes
    ("0xc36442b4a4522e871399cd717abdd847ab11fe88", "uniswap_v3", "dex")
    , ("0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45", "uniswap_v3", "dex")
    , ("0x58a3c68e2d3aaf316239c003779f71acb870ee47", "curvefi", "dex")
	-- lending protocols
    , ("0xba5ebaf3fc1fcca67147050bf80462393814e54b", "alpha_homora", "lending")
    , ("0x5f5cd91070960d13ee549c9cc47e7a4cd00457bb", "alpha_homora_v2", "lending")
    ) AS tmp_table (address, project, project_type)
