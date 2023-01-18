{{ config(
    alias = 'staking_entities',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['address'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "ethereum",
                                \'["hildobby"]\') }}')
}}

SELECT address, entity, category
FROM
  (VALUES
  ('0xae7ab96520de3a18e5e111b5eaab095312d7fe84', 'Lido', 'Liquid Staking')
    , ('0x39dc6a99209b5e6b81dc8540c86ff10981ebda29', 'Staked.us', 'Staking Pools')
    , ('0x0194512e77d798e4871973d9cb9d7ddfc0ffd801', 'stakefish', 'Staking Pools')
    , ('0xd4039ecc40aeda0582036437cf3ec02845da4c13', 'Kraken', 'CEX')
    , ('0xa40dfee99e1c85dc97fdc594b16a460717838703', 'Kraken', 'CEX')
    , ('0x622de9bb9ff8907414785a633097db438f9a2d86', 'Bitcoin Suisse', 'Staking Pools')
    , ('0xdd9663bd979f1ab1bada85e1bc7d7f13cafe71f8', 'Bitcoin Suisse', 'Staking Pools')
    , ('0xec70e3c8afe212039c3f6a2df1c798003bf7cfe9', 'Bitcoin Suisse', 'Staking Pools')
    , ('0x3837ea2279b8e5c260a78f5f4181b783bbe76a8b', 'Bitcoin Suisse', 'Staking Pools')
    , ('0x2a7077399b3e90f5392d55a1dc7046ad8d152348', 'Bitcoin Suisse', 'Staking Pools')
    , ('0xc2288b408dc872a1546f13e6ebfa9c94998316a2', 'Bitcoin Suisse', 'Staking Pools')
    , ('0xf2be95116845252a28bd43661651917dc183dab1', 'Figment', 'Staking Pools')
    , ('0x37ab162ab59e106d6072eb7a7bd4c4c2973455a7', 'Figment', 'Staking Pools')
    , ('0xc874b064f465bdd6411d45734b56fac750cda29a', 'Stakewise', 'Liquid Staking')
    , ('0x84db6ee82b7cf3b47e8f19270abde5718b936670', 'Stkr (Ankr)', 'Liquid Staking')
    , ('0x194bd70b59491ce1310ea0bceabdb6c23ac9d5b2', 'Huobi', 'CEX')
    , ('0xb73f4d4e99f65ec4b16b684e44f81aeca5ba2b7c', 'Huobi', 'CEX')
    , ('0xbf1556a7d625654e3d64d1f0466a60a697fac178', 'InfStones', 'Staking Pools')
    , ('0xbca3b7b87dcb15f0efa66136bc0e4684a3e5da4d', 'SharedStake', 'Liquid Staking')
    , ('0xeadcba8bf9aca93f627f31fb05470f5a0686ceca', 'StakeWise Solos', 'Staking Pools')
    , ('0xfa5f9eaa65ffb2a75de092eb7f3fc84fc86b5b18', 'Abyss Finance', 'Staking Pools')
    , ('0x66827bcd635f2bb1779d68c46aeb16541bca6ba8', 'PieDAO', 'Staking Pools')
    , ('0xd6216fc19db775df9774a6e33526131da7d19a2c', 'KuCoin', 'CEX')
    , ('0x1692e170361cefd1eb7240ec13d048fd9af6d667', 'KuCoin', 'CEX')
    , ('0x7b915c27a0ed48e2ce726ee40f20b2bf8a88a1b3', 'KuCoin', 'CEX')
    , ('0xcbc1065255cbc3ab41a6868c22d1f1c573ab89fd', 'CREAM', 'Liquid Staking')
    , ('0x808e7133c700cf3a66e6a25aadb1fbef6be468b4', 'Bitstamp', 'CEX')
    , ('0x12ec5befa9166fa327d4c345a93f0ac99dd2a7d8', 'Blox Staking', 'Staking Pools')
    , ('0x24b2f1aeced4b34133152bb20cfd6a206a0ea33c', 'staked.finance', 'Liquid Staking')
    , ('0x0ca7b4b87feb2c0bda9cb37b6cd0de22b816cd04', 'MyColdWallet', 'Others')
    , ('0x1270a0aad453a315c5ab99397d88121c34453eb4', 'TideBit', 'Others')
    , ('0x0038598ecb3b308ebc6c6e2c635bacaa3c5298a3', 'Poloniex', 'CEX')
    , ('0xd39aeeb73983e5fbc52b77a3957a48c1eeac8ed7', 'MintDice.com', 'Others')
    , ('0x7ebf05749faf7eb78eff153e40c15890bb4578a4', 'neukind.com', 'Staking Pools')
    , ('0xa54be2edaa143e969a63fc744bbd2d511b50cbc3', 'neukind.com', 'Staking Pools')
    , ('0xac29ef7a7f4325ffa564de9abf67e5ace46c88f8', 'neukind.com', 'Staking Pools')
    , ('0xc3003f8b89f35a7bf3cb3a6ec3d8e4c3c8ce7cce', 'neukind.com', 'Staking Pools')
    , ('0x06521af7183a4a61d067016fc3bc0500da1567c1', 'ptxptx', 'Others')
    , ('0x8e1d8b147cc4c939a597dc501c47cc8b4ab26bd5', 'Tetranode', 'Whales')
    , ('0x1db3439a222c519ab44bb1144fc28167b4fa6ee6', 'Vitalik Buterin', 'Whales')
    , ('0x49df3cca2670eb0d591146b16359fe336e476f29', 'stereum.net', 'Others')
    , ('0x62dfeb55fcbdcb921446168eecfd1406379a1ee1', 'stereum.net', 'Others')
    , ('0x2be0282532ad9fa7cc4c45aeaa1707d2e93357c2', 'Blockdaemon.com', 'Others')
    , ('0x5e59aab1f114234f003008300c3d7593c6ceea26', 'boxfish.studio', 'Others')
    , ('0xd1933df1c223ad7cb5716b066ca26bc24569e622', 'Ethereum on ARM', 'Others')
    , ('0x5a0036bcab4501e70f086c634e2958a8beae3a11', 'OKX', 'CEX')
    , ('0x607ebc82329d0cac3027b83d15e4b4e816f131b7', 'StakeHound', 'Liquid Staking')
    , ('0xc236c3ec83351b07f148afbadc252cce2c07972e', 'Bitfinex', 'CEX')
    , ('0xe733455faddf4999176e99a0ec084e978f5552ed', 'Bitfinex', 'CEX')
    , ('0x4c2f150fc90fed3d8281114c2349f1906cde5346', 'Gemini', 'CEX')
    , ('0xbb84d966c09264ce9a2104a4a20bb378369986db', 'WEX Exchange', 'CEX')
    , ('0xe0c8df4270f4342132ec333f6048cb703e7a9c77', 'Swell', 'Liquid Staking')
    , ('0xbafa44efe7901e04e39dad13167d089c559c1138', 'Frax Finance', 'Liquid Staking')
    , ('0xefe9a82d56cd965d7b332c7ac1feb15c53cd4340', 'stakefish', 'Staking Pools')
    , ('0xeee27662c2b8eba3cd936a23f039f3189633e4c8', 'Celsius', 'Staking Pools')
    ) 
    x (address, entity, category)
    UNION ALL
    SELECT coinbase.address
    , 'Coinbase' AS name
    , 'CEX' AS category
    FROM (
        SELECT et.from AS address
        FROM {{ source('ethereum', 'traces') }} et
        INNER JOIN {{ source('ethereum', 'traces') }} et2 ON et2.from=et.from
            AND et2.to='0xa090e606e30bd747d4e6245a1517ebe430f0057e'
            AND et2.block_time >= '2020-10-14'
        WHERE et.to='0x00000000219ab540356cbb839cbe05303d7705fa'
        AND et.success
        AND et.block_time >= '2020-10-14'
        {% if is_incremental() %}
        AND et.block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
        GROUP BY et.from
        ) coinbase
    GROUP BY coinbase.address
    UNION ALL
    SELECT binance.address
    , 'Binance' AS name
    , 'CEX' AS category
    FROM (
        SELECT '0xf17aced3c7a8daa29ebb90db8d1b6efd8c364a18' AS address
        UNION ALL
        SELECT '0x2f47a1c2db4a3b78cda44eade915c3b19107ddcc' AS address
        UNION ALL
        SELECT distinct to AS address
        FROM {{ source('ethereum', 'transactions') }}
        WHERE from='0xf17aced3c7a8daa29ebb90db8d1b6efd8c364a18'
        AND to !='0x00000000219ab540356cbb839cbe05303d7705fa'
        AND block_time >= '2020-10-14'
        {% if is_incremental() %}
        AND block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
        GROUP BY to
    ) binance
    LEFT JOIN {{ source('ethereum', 'traces') }} t ON binance.address=t.from AND t.to='0x00000000219ab540356cbb839cbe05303d7705fa'
    GROUP BY binance.address
    ;