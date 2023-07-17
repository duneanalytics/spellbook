{{config(alias = alias('defi'),
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "sector",
                                    "addresses",
                                    \'["umer_h_adil"]\') }}')}}

SELECT address, project, project_type
FROM (VALUES
	-- LP Positions
    ("0xc36442b4a4522e871399cd717abdd847ab11fe88", "uniswap_v3", "liquidity_pool")
    , ("0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45", "uniswap_v3", "liquidity_pool")
    , ("0x58a3c68e2d3aaf316239c003779f71acb870ee47", "curvefi", "liquidity_pool")
    , ("0xa4492fcda2520cb68657d220f4d4ae3116359c10", "premia", "liquidity_pool")
    , ("0x1b63334f7bfdf0d753ab3101eb6d02b278db8852", "premia", "liquidity_pool")
    , ("0xfdd2fc2c73032ae1501ef4b19e499f2708f34657", "premia", "liquidity_pool")
    , ("0x4fcb6363acf29133540f85df2f2a0cab9eefd3c0", "premia", "liquidity_pool")
    , ("0x2fb48b41e3bf0c86e8a90e7ca168e6b63622855f", "premia", "liquidity_pool")
    , ("0xb7cd1bb23c69b6becdf5aa0fe17c444db67f5d93", "premia", "liquidity_pool")
    , ("0x9998ca8ea9e39d5c84a171ecb3303674e666ef9c", "premia", "liquidity_pool")
	-- lending protocols
    , ("0xba5ebaf3fc1fcca67147050bf80462393814e54b", "alpha_homora", "lending")
    , ("0x5f5cd91070960d13ee549c9cc47e7a4cd00457bb", "alpha_homora_v2", "lending")
    ) AS tmp_table (address, project, project_type)
