{{ config(
    schema = 'spectra_arbitrum',
    alias = 'base_trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index']
) }}

WITH token_map AS (
    SELECT * FROM (VALUES
    (0x0329b7bac6e908afb944e521bd63e5e205ef3166, 0x0d15225454474ab3cb124083278c7be03f8a99ff, 0x8eda3efd3e73fd91c410f685cbf43100ee260b1f),
    (0x0bdad9b3a7aa822c9f846b6490fe526dbdfdda3e, 0x0022228a2cc5e7ef0274a7baa600d44da5ab5776, 0x1eefc351c76e34cba20cd7c0844463bf4e6fba25),
    (0x16a6746b1ffdb14e8f88e3082f23d956e68ff9da, 0x24174022d382cd155c33a847404cda5bc7978802, 0x5299f03ff29b0d850630466188f05aade3699157),
    (0x1dd42dd08af91ef8bcbb55c1f11c9239a101237e, 0x1c5ecca381961d92b6aaf7bc1656c37021b0f1d9, 0xfc960feb069ccdaffbbd03050f0b6c9be29a25c9),
    (0x39e6af30ea89034d1bdd2d1cfba88caf8464fa65, 0x85050bedc80ea28e53db5f80f165d87f29d2a1bc, 0x95590e979a72b6b04d829806e8f29aa909ed3a86),
    (0x4455e524ba0d45a7671b5b705818696d1c0e6cb7, 0x0022228a2cc5e7ef0274a7baa600d44da5ab5776, 0x75d877a6241256364a682ccf332ad4b3bcbffffb),
    (0x4d4140c05dfa65e3e38a7bf1bda6e5b68fbe2b80, 0x2dabcea55a12d73191aece59f508b191fb68adac, 0x6def54ae7e38992a7d1ab60d279483ba7f7b0aeb),
    (0x5711f2aab169d6f83d997008542863606530c5a5, 0xd85e038593d7a098614721eae955ec2022b9b91b, 0x52552d18c71487d17ad4a0de91185f7bfb222a55),
    (0x6293e840c02a2939cb000d002de8b70b5b7e62b8, 0x7e32f4c44e22ab20df287f8a15eb6c0f54da6e30, 0x5babedf388dd444fb237e5025f9d02831a0ba927),
    (0x83ffe303a62d25a7d1a4c2302d3ea24b59542bba, 0xc76387330f1614d7e10e045cfcc7c9f7ff2adc1f, 0x78c4b41ac0c5e45a0ce5616a9580dfd1953ef1db),
    (0x96839ee35a8e9f554f06042bcaf1b8962d63f8ef, 0x2b70238022589f17e7b266bc753e74027d57009f, 0x3c90186e9415842d7ea896a30932c9c0d835ba33),
    (0xbfdfa1d1b90eb7715100acd4f69d4d2828b8cbc5, 0xed5f727107bdac99443bae317e0ef38239719e87, 0x6b73ee455d1076ea680c01721afe346c2a0dd844),
    (0xc515063419fd6ba60c8206f591f1c93bb1518765, 0x02ab76b19944bdec4d4c612468bb6af3ac76b15d, 0x56442bbc33a313f247e401b7ef3659c6f9e3ff14),
    (0xd20ab8a3f61001e8b9ebe2a39b85d5b2c232b040, 0x493aa6240130ed92e2b81fcfbfee500b924ca296, 0x52d5173f0c59d1082a67303765d840c52c5dcf61),
    (0xdc2b69aa3c13cab8010b8cc3c4e838b2b005089f, 0x6faf8b7ffee3306efcfc2ba9fec912b4d49834c1, 0x054db0e66ed52d554bc549df83abb632555d4d9b),
    (0xdc3b1c7cd554036d10fe2ced605b349a8c831152, 0x32db5cbac1c278696875eb9f27ed4cd7423dd126, 0x4fef358197d31032dd84a28430673f92e8785db1),
    (0xe62ec36665387fd9abc303c0aea161c26649b6a3, 0xea50f402653c41cadbafd1f788341db7b7f37816, 0xeb64a82e17314c4eb73e7d0a2611fde71c0e09af),
    (0xe8ce8988c227cec35c03bef8a899a5673f4eb66e, 0x24174022d382cd155c33a847404cda5bc7978802, 0x8fdc1e3e392bcddf6149c7e920d8db3483f168cd),
    (0xf206c1e996536d8e29985e6663e447a27e19e8d3, 0x710a1ab6cb8412de9613ad6c7195453ce8b5ca71, 0xe53a05010603aaae8e8402cce16f4d43b2be8a25),
    (0xf327a464da7691ff5fb25cd233813c7a958a6449, 0x2ec0160246461f0ce477887dde2c931ee8233de7, 0xd97c1baa5547a0e22b34b917492c843a24a5cc42),
    (0xf5650b7dd5216fcc6cfa5c009ffd147ed8254f37, 0xdfd2214236b60fc0485288c959cb07da4f6a15f7, 0x458e662aa15a3e338a9b1f9668e2ccb302fd4340)
    ) AS t(contract_address, token_address_0, token_address_1)
),


token_swaps AS (
    SELECT
        vcc.evt_block_number AS block_number,
        -- CAST(vcc.evt_block_time AS timestamp(3) with time zone) AS block_time,
        TRY_CAST(vcc.evt_block_time AS timestamp(3) with time zone) AS block_time,
        vcc.buyer AS maker,
        vcc.evt_tx_to AS taker,
        vcc.tokens_sold AS token_sold_amount_raw,
        vcc.tokens_bought AS token_bought_amount_raw,
        vcc.sold_id,
        vcc.bought_id,
        vcc.contract_address,
        vcc.evt_tx_hash AS tx_hash,
        vcc.evt_index AS evt_index,
        tm.token_address_0,
        tm.token_address_1
    FROM {{ source('spectra_multichain', 'vyper_contract_evt_tokenexchange') }} vcc
    LEFT JOIN token_map tm
        ON vcc.contract_address = tm.contract_address
    WHERE vcc.chain = 'arbitrum'
    {% if is_incremental() %}
        AND {{ incremental_predicate('evt_block_time') }}
    {% endif %}
)

SELECT
    'arbitrum' AS blockchain,
    'spectra' AS project,
    '1' AS version,
    -- CAST(date_trunc('month', ts.block_time) AS date) AS block_month,
    -- CAST(date_trunc('day', ts.block_time) AS date) AS block_date,
    TRY_CAST(date_trunc('month', ts.block_time) AS date) AS block_month,
    TRY_CAST(date_trunc('day', ts.block_time) AS date) AS block_date,
    ts.block_time,
    ts.block_number,
    ts.token_sold_amount_raw,
    ts.token_bought_amount_raw,

    -- 🧠 Conditional logic based on sold_id and bought_id
    CASE WHEN ts.sold_id = 0 THEN ts.token_address_0
         WHEN ts.sold_id = 1 THEN ts.token_address_1
         ELSE NULL END AS token_sold_address,

    CASE WHEN ts.bought_id = 0 THEN ts.token_address_0
         WHEN ts.bought_id = 1 THEN ts.token_address_1
         ELSE NULL END AS token_bought_address,

    ts.maker,
    ts.taker,
    ts.contract_address AS project_contract_address,
    ts.tx_hash,
    ts.evt_index
FROM token_swaps ts
