{{ config(
    schema='relay',
    alias='token_transfers',
    materialized='view'
) }}

WITH  relay_tokens (symbol, contract_address, decimals, blockchain) as (values 

('ETH', 0x0000000000000000000000000000000000000000, 18, 'ethereum'),
('USDC', 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48, 6, 'ethereum'),
('USDT', 0xdac17f958d2ee523a2206206994597c13d831ec7, 6, 'ethereum'),
('WETH', 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2, 18, 'ethereum'),
('WBTC', 0x2260fac5e5542a773aa44fbcfedf7c193bc2c599, 8, 'ethereum'),
('DAI', 0x6b175474e89094c44da98b954eedeac495271d0f, 18, 'ethereum'),
('PLUME', 0x4c1746a800d224393fe2470c70a35717ed4ea5f1, 18, 'ethereum'),
('G7', 0x12c88a3c30a7aabc1dd7f2c08a97145f5dccd830, 18, 'ethereum'),
('ANIME', 0x4dc26fc5854e7648a064a4abd590bbe71724c277, 18, 'ethereum'),
('GOD', 0xb5130f4767ab0acc579f25a76e8f9e977cb3f948, 18, 'ethereum'),
('cbBTC', 0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf, 8, 'ethereum'),
('TOPIA', 0xccccb68e1a848cbdb5b60a974e07aae143ed40c3, 18, 'ethereum'),
('APE', 0x4d224452801aced8b2f0aebe155379bb5d594381, 18, 'ethereum'),
('POWER', 0x429f0d8233e517f9acf6f0c8293bf35804063a83, 18, 'ethereum'),
('OMI', 0xed35af169af46a02ee13b9d79eb57d6d68c1749e, 18, 'ethereum'),
('SIPHER', 0x9f52c8ecbee10e00d9faaac5ee9ba0ff6550f511, 18, 'ethereum'),
('G', 0x9c7beba8f6ef6643abd725e45a4e8387ef260649, 18, 'ethereum'),
('ETH', 0x0000000000000000000000000000000000000000, 18, 'optimism'),
('USDC', 0x0b2c639c533813f4aa9d7837caf62653d097ff85, 6, 'optimism'),
('USDT', 0x94b008aa00579c1307b0ef2c499ad98a8ce58e58, 6, 'optimism'),
('WETH', 0x4200000000000000000000000000000000000006, 18, 'optimism'),
('WBTC', 0x68f180fcce6836688e9084f035309e29bf0a2095, 8, 'optimism'),
('SIPHER', 0xb94944669f7967e16588e55ac41be0d5ef399dcd, 18, 'optimism'),
('DAI', 0xda10009cbd5d07dd0cecc66161fc93d7c9000da1, 18, 'optimism'),
('CRO', 0x0000000000000000000000000000000000000000, 18, 'cronos'),
('USDC', 0xc21223249ca28397b4b6541dffaecc539bff0c59, 6, 'cronos'),
('BNB', 0x0000000000000000000000000000000000000000, 18, 'bnb'),
('USDC', 0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d, 18, 'bnb'),
('USDT', 0x55d398326f99059ff775485246999027b3197955, 18, 'bnb'),
('G', 0x9c7beba8f6ef6643abd725e45a4e8387ef260649, 18, 'bnb'),
('DAI', 0x1af3f329e8be154074d8769d1ffa4ee058b1dbc3, 18, 'bnb'),
('xDAI', 0x0000000000000000000000000000000000000000, 18, 'gnosis'),
('USDC', 0x2a22f9c3b484c3629090feed35f17ff8f88f76f0, 6, 'gnosis'),
('ETH', 0x0000000000000000000000000000000000000000, 18, 'unichain'),
('USDC', 0x078d782b760474a361dda0af3839290b0ef57ad6, 6, 'unichain'),
('POL', 0x0000000000000000000000000000000000000000, 18, 'polygon'),
('USDC', 0x3c499c542cef5e3811e1192ce70d8cc03d5c3359, 6, 'polygon'),
('USDT', 0xc2132d05d31c914a87c6611c10748aeb04b58e8f, 6, 'polygon'),
('WBTC', 0x1bfd67037b42cf73acf2047067bd4f2c47d9bfd6, 8, 'polygon'),
('DAI', 0x8f3cf7ad23cd3cadbd9735aff958023239c6a063, 18, 'polygon'),
('S', 0x0000000000000000000000000000000000000000, 18, 'sonic'),
('USDC', 0x29219dd400f2bf60e5a23d13be72b486d4038894, 6, 'sonic'),
('ETH', 0x0000000000000000000000000000000000000000, 18, 'manta-pacific'),
('ETH', 0x0000000000000000000000000000000000000000, 18, 'mint'),
('ETH', 0x0000000000000000000000000000000000000000, 18, 'boba'),
('ETH', 0x0000000000000000000000000000000000000000, 18, 'zksync'),
('ETH', 0x0000000000000000000000000000000000000000, 18, 'shape'),
('ETH', 0x0000000000000000000000000000000000000000, 18, 'appchain'),
('USDC', 0x675c3ce7f43b00045a4dab954af36160fb57cb45, 6, 'appchain'),
('ETH', 0x0000000000000000000000000000000000000000, 18, 'world-chain'),
('ETH', 0x0000000000000000000000000000000000000000, 18, 'redstone'),
('FLOW', 0x0000000000000000000000000000000000000000, 18, 'flow-evm'),
('USDC', 0xf1815bd50389c46847f0bda824ec8da914045d14, 6, 'flow-evm'),
('ETH', 0x1fbccdc677c10671ee50b46c61f0f7d135112450, 18, 'hyperevm'),
('METIS', 0x0000000000000000000000000000000000000000, 18, 'metis'),
('WETH', 0x420000000000000000000000000000000000000a, 18, 'metis'),
('ETH', 0x0000000000000000000000000000000000000000, 18, 'polygon-zkevm'),
('ETH', 0x0000000000000000000000000000000000000000, 18, 'lisk'),
('SEI', 0x0000000000000000000000000000000000000000, 18, 'sei'),
('USDC', 0x3894085ef7ff0f0aedf52e2a2704928d1ec074f1, 6, 'sei'),
('ETH', 0x0000000000000000000000000000000000000000, 18, 'perennial'),
('USDC', 0x39cd9ef9e511ec008247ad5da01245d84a9521be, 6, 'perennial'),
('IP', 0x0000000000000000000000000000000000000000, 18, 'story'),
('USDC', 0xf1815bd50389c46847f0bda824ec8da914045d14, 6, 'story'),
('WETH', 0xbab93b7ad7fe8692a878b95a8e689423437cc500, 18, 'story'),
('G', 0x0000000000000000000000000000000000000000, 18, 'gravity'),
('ETH', 0x0000000000000000000000000000000000000000, 18, 'soneium'),
('USDC.e', 0xba9986d2381edf1da03b0b9c1f8b00dc4aacc369, 6, 'soneium'),
('ETH', 0x0000000000000000000000000000000000000000, 18, 'swellchain'),
('DMT', 0x0000000000000000000000000000000000000000, 18, 'sanko'),
('RON', 0x0000000000000000000000000000000000000000, 18, 'ronin'),
('USDC', 0x0b7007c13325c48911f73a2dad5fa5dcbf808adc, 6, 'ronin'),
('G7', 0x0000000000000000000000000000000000000000, 18, 'game7'),
('USDC', 0x401ecb1d350407f13ba348573e5630b83638e30d, 6, 'game7'),
('ETH', 0x0000000000000000000000000000000000000000, 18, 'abstract'),
('USDC', 0x84a71ccd554cc1b02749b35d22f684cc8ec987e1, 6, 'abstract'),
('PENGU', 0x9ebe3a824ca958e4b3da772d2065518f009cba62, 18, 'abstract'),
('ETH', 0x0000000000000000000000000000000000000000, 18, 'morph'),
('TOPIA', 0x0000000000000000000000000000000000000000, 18, 'hychain'),
('USDC', 0x0000000000000000000000000000000000000000, 18, 'echos'),
('MNT', 0x0000000000000000000000000000000000000000, 18, 'mantle'),
('USDC', 0x09bc4e0d864854c6afb6eb9a9cdf58ac190d0df9, 6, 'mantle'),
('ETH', 0x0000000000000000000000000000000000000000, 18, 'ham'),
('ETH', 0x0000000000000000000000000000000000000000, 18, 'superseed'),
('ETH', 0x0000000000000000000000000000000000000000, 18, 'cyber'),
('POWER', 0x0000000000000000000000000000000000000000, 18, 'powerloom-v2'),
('ETH', 0x0000000000000000000000000000000000000000, 18, 'arena-z'),
('ETH', 0x0000000000000000000000000000000000000000, 18, 'b3'),
('USDC', 0x2af198a85f9aa11cd6042a0596fbf23978514da3, 6, 'b3'),
('ETH', 0x0000000000000000000000000000000000000000, 18, 'base'),
('USDC', 0x833589fcd6edb6e08f4c7c32d4f71b54bda02913, 6, 'base'),
('USDT', 0xfde4c96c8593536e31f229ea8f37b2ada2699bb2, 6, 'base'),
('WETH', 0x4200000000000000000000000000000000000006, 18, 'base'),
('DEGEN', 0x4ed4e862860bed51a9570b96d89af5e1b0efefed, 18, 'base'),
('GOD', 0xb5130f4767ab0acc579f25a76e8f9e977cb3f948, 18, 'base'),
('POP', 0xc9ef0e04038f64d6f759bd73b4b1cb6c78c59daa, 18, 'base'),
('cbBTC', 0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf, 8, 'base'),
('LRDS', 0xb676f87a6e701f0de8de5ab91b56b66109766db1, 18, 'base'),
('OMI', 0x3792dbdd07e87413247df995e692806aa13d3299, 18, 'base'),
('DAI', 0x50c5725949a6f0c72e6c4a641f24049a917db0cb, 18, 'base'),
('SIPHER', 0xd0d1e44fc9adaeb732f73ffc2429cd1db9cd4529, 18, 'base'),
('POP', 0x0000000000000000000000000000000000000000, 18, 'onchain-points'),
('APE', 0x0000000000000000000000000000000000000000, 18, 'apechain'),
('ApeETH', 0xcf800f4948d16f23333508191b1b1591daf70438, 18, 'apechain'),
('ETH', 0x0000000000000000000000000000000000000000, 18, 'funki'),
('SIPHER', 0x7d8b6cec10165119c4ac7843a1e02184789585d8, 18, 'funki'),
('ETH', 0x0000000000000000000000000000000000000000, 18, 'mode'),
('ETH', 0x0000000000000000000000000000000000000000, 18, 'arbitrum'),
('USDC', 0xaf88d065e77c8cc2239327c5edb3a432268e5831, 6, 'arbitrum'),
('USDT', 0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9, 6, 'arbitrum'),
('WETH', 0x82af49447d8a07e3bd95bd0d56f35241523fbab1, 18, 'arbitrum'),
('WBTC', 0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f, 8, 'arbitrum'),
('XAI', 0x4cb9a7ae498cedcbb5eae9f25736ae7d428c9d66, 18, 'arbitrum'),
('G7', 0xf18e4466f26b4ca55bbab890b314a54976e45b17, 18, 'arbitrum'),
('ANIME', 0x37a645648df29205c6261289983fb04ecd70b4b3, 18, 'arbitrum'),
('cbBTC', 0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf, 8, 'arbitrum'),
('DMT', 0x8b0e6f19ee57089f7649a455d89d7bc6314d04e8, 18, 'arbitrum'),
('APE', 0x7f9fbf9bdd3f4105c478b996b648fe6e828a1e98, 18, 'arbitrum'),
('ETH', 0x0000000000000000000000000000000000000000, 18, 'arbitrum-nova'),
('GOD', 0xb5130f4767ab0acc579f25a76e8f9e977cb3f948, 18, 'arbitrum-nova'),
('CELO', 0x0000000000000000000000000000000000000000, 18, 'celo'),
('USDC', 0xceba9300f2b948710d2653dd7b07f33a8b32118c, 6, 'celo'),
('ETH', 0x0000000000000000000000000000000000000000, 18, 'hemi'),
('AVAX', 0x0000000000000000000000000000000000000000, 18, 'avalanche'),
('USDC', 0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e, 6, 'avalanche'),
('GUN', 0x26debd39d5ed069770406fca10a0e4f8d2c743eb, 18, 'avalanche'),
('ETH', 0x0000000000000000000000000000000000000000, 18, 'zircuit'),
('ETH', 0x0000000000000000000000000000000000000000, 18, 'superposition'),
('ETH', 0x0000000000000000000000000000000000000000, 18, 'ink'),
('ETH', 0x0000000000000000000000000000000000000000, 18, 'linea'),
('ETH', 0x0000000000000000000000000000000000000000, 18, 'bob'),
('ANIME', 0x0000000000000000000000000000000000000000, 18, 'animechain'),
('USDC', 0x401ecb1d350407f13ba348573e5630b83638e30d, 6, 'animechain'),
('ETH', 0x0000000000000000000000000000000000000000, 18, 'apex'),
('ETH', 0x0000000000000000000000000000000000000000, 18, 'boss'),
('BERA', 0x0000000000000000000000000000000000000000, 18, 'berachain'),
('USDC', 0x549943e04f40284185054145c6e4e9568c1d3241, 6, 'berachain'),
('WETH', 0x2f6f07cdcf3588944bf4c42ac74ff24bf56e7590, 18, 'berachain'),
('ETH', 0x0000000000000000000000000000000000000000, 18, 'blast'),
('WETH', 0x4300000000000000000000000000000000000004, 18, 'blast'),
('ETH', 0x0000000000000000000000000000000000000000, 18, 'taiko'),
('ETH', 0x0000000000000000000000000000000000000000, 18, 'scroll'),
('ETH', 0x0000000000000000000000000000000000000000, 18, 'zero-network'),
('USDC', 0x6a6394f47dd0baf794808f2749c09bd4ee874e70, 6, 'zero-network'),
('XAI', 0x0000000000000000000000000000000000000000, 18, 'xai'),
('TIA', 0x0000000000000000000000000000000000000000, 18, 'forma'),
('ETH', 0x0000000000000000000000000000000000000000, 18, 'zora'),
('BTCN', 0x0000000000000000000000000000000000000000, 18, 'corn'),
('USDC', 0xdf0b24095e15044538866576754f3c964e902ee6, 6, 'corn'),
('WBTCN', 0xda5ddd7270381a7c2717ad10d1c0ecb19e3cdfb2, 18, 'corn'),
('DEGEN', 0x0000000000000000000000000000000000000000, 18, 'degen'),
('ETH', 0x0000000000000000000000000000000000000000, 18, 'ancient8'),
('ETH', 0x0000000000000000000000000000000000000000, 18, 'rari') ),


excluded_addresses AS (
  SELECT * FROM (
    VALUES 
      (0xeeeeee9eC4769A09a76A83C7bC42b185872860eE),
      (0x000000000000000000000000000000000000800a),
      (0x0000000000000000000000000000000000008001),
      (0x0000000000000000000000000000000000000000)
  ) AS t(address)
),

txs AS (
  SELECT block_time, blockchain, hash, block_number
  FROM {{ source('evms', 'transactions') }}
  WHERE "from" = 0xf70da97812CB96acDF810712Aa562db8dfA3dbEF
    AND success = TRUE
),

eth_transfers AS (
    SELECT
    DATE_TRUNC('day', e.block_time) AS day,
    e.blockchain,
    e.hash AS evt_tx_hash,
    e.block_time AS evt_block_time,
    e."to",
    CASE
        WHEN e.blockchain = 'polygon' THEN 'MATIC'
        WHEN e.blockchain = 'avalanche_c' THEN 'AVAX'
        WHEN e.blockchain = 'gnosis' THEN 'xDAI'
        WHEN e.blockchain = 'bnb' THEN 'BNB'
        WHEN e.blockchain = 'fantom' THEN 'FTM'
        WHEN e.blockchain = 'celo' THEN 'CELO'
        WHEN e.blockchain = 'mantle' THEN 'MNT'
        WHEN e.blockchain = 'mode' THEN 'MODE'
        ELSE 'ETH'
    END AS symbol,
    e.value / 1e18 AS value,
    p.price,
    (e.value / 1e18) * p.price AS amount
FROM {{ source('evms', 'transactions') }} e
LEFT JOIN {{ source('prices', 'day') }} p
    ON DATE_TRUNC('day', e.block_time) = p.timestamp
    AND e.blockchain = p.blockchain
    AND p.symbol = CASE
        WHEN e.blockchain = 'polygon' THEN 'MATIC'
        WHEN e.blockchain = 'avalanche_c' THEN 'AVAX'
        WHEN e.blockchain = 'gnosis' THEN 'xDAI'
        WHEN e.blockchain = 'bnb' THEN 'BNB'
        WHEN e.blockchain = 'fantom' THEN 'FTM'
        WHEN e.blockchain = 'celo' THEN 'CELO'
        WHEN e.blockchain = 'mantle' THEN 'MNT'
        WHEN e.blockchain = 'mode' THEN 'MODE'
        ELSE 'ETH'
    END
WHERE e."from" = 0xf70da97812CB96acDF810712Aa562db8dfA3dbEF

AND e."to" NOT IN (SELECT address FROM excluded_addresses)
AND e.success = TRUE
AND e.value > 0),

erc20_transfers AS (
  SELECT
    DATE_TRUNC('day', e.evt_block_time) AS day,
    e.blockchain,
    e.evt_tx_hash,
    e.evt_block_time,
    e."to",
    t.symbol,
    e.value / POWER(10, t.decimals) AS value,
    p.price,
    (e.value / POWER(10, t.decimals)) * p.price AS amount
  FROM {{ source('evms', 'erc20_transfers') }} e
  INNER JOIN relay_tokens t
    ON e.contract_address = t.contract_address AND e.blockchain = t.blockchain
  INNER JOIN txs tx
    ON e.evt_tx_hash = tx.hash AND e.blockchain = tx.blockchain
  LEFT JOIN {{ source('prices', 'day') }} p
    ON e.blockchain = p.blockchain
    AND DATE_TRUNC('day', e.evt_block_time) = p.timestamp
    AND e.contract_address = p.contract_address
  WHERE e."from" IN (
      0xf70da97812CB96acDF810712Aa562db8dfA3dbEF,
      0xeeeeee9eC4769A09a76A83C7bC42b185872860eE,
      0xe0b062d028236fa09fe33db8019ffeeee6bf79ed,
      0xa1bea5fe917450041748dbbbe7e9ac57a4bbebab,
      0x435bc1fa302256f0c4b704ae3b7ff322d5c1490c,
      0xf605a9345401a0a3caacbcf9d891949218e0142d,
      0x3db750fd20d09f7988b7331f412a516c524caf13,
      0xa72dc2c494b030894699fa081b8974bdb094d3bd,
      0x0649f2daa72a6524f0eb7f5f65e13cffc8082b10,
      0x1d3a594eaf472ca2cec2a8ae44478c06d6a37e22,
      0x610f3927901c41c219a1ad267df8073dbc883464,
      0x6b7b5e5a75eb1c26689618c571143d255694c134,
      0x85412871bce7432b9bd32eb9ff8c58997dd5a96c,
      0x57732abcad29648c977c18a39bdf474436bba973,
      0xe377e13256002ab260e8ab59478652710a79ac5c,
      0x836caf2409d91df0bda01bc9f3cec524ba1c571d
    )
    AND e."to" NOT IN (SELECT address FROM excluded_addresses)
    AND e.value > 0
)

SELECT DISTINCT *
FROM (
  SELECT * FROM erc20_transfers
  UNION ALL
  SELECT * FROM eth_transfers
) combined_transfers
