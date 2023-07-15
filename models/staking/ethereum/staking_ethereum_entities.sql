{{ config(
    alias = alias('entities'),
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['address'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "sector",
                                "staking",
                                \'["hildobby"]\') }}')
}}

WITH contracts AS (
    SELECT lower(trim(address)) as address, trim(entity) as entity, trim(category) as category
    FROM
    (VALUES
    ('0xdcd51fc5cd918e0461b9b7fb75967fdfd10dae2f', 'Rocket Pool', 'Liquid Staking')
    , ('0x1cc9cf5586522c6f483e84a19c3c2b0b6d027bf0', 'Rocket Pool', 'Liquid Staking')
    , ('0x2fb42ffe2d7df8381853e96304300c6a5e846905', 'Rocket Pool', 'Liquid Staking')
    , ('0x9b8c989ff27e948f55b53bb19b3cc1947852e394', 'Kiln', 'Staking Pools')
    , ('0x1e68238ce926dec62b3fbc99ab06eb1d85ce0270', 'Kiln', 'Staking Pools')
    , ('0x2421a0af8badfae12e1c1700e369747d3db47b09', 'SenseiNode', 'Staking Pools')
    , ('0x10e02a656b5f9de2c44c687787c36a2c4801cc40', 'Tranchess', 'Liquid Staking')
    , ('0x447c3ee829a3B506ad0a66Ff1089F30181c42637', 'KingHash', 'Liquid Staking')
    , ('0xa8f50a6c41d67685b820b4fe6bed7e549e54a949', 'Eth2Stake', 'Staking Pools')
    , ('0xf243a92eb7d4b4f6a00a57888b887bd01ec6fd12', 'MyEtherWallet', 'Staking Pools')
    , ('0x73fd39ba4fb23c9b080fca0fcbe4c8c7a2d630d0', 'MyEtherWallet', 'Staking Pools')
    , ('0xe7b385fb5d81259280b7d639df81513ab8b005e4', 'MyEtherWallet', 'Staking Pools')
    , ('0x82ce843130ff0ae069c54118dfbfa6a5ea17158e', 'Gemini', 'CEX')
    , ('0x24d729aae93a05a729e68504e5ccdfa3bb876491', 'Gemini', 'CEX')
        ) 
        x (address, entity, category)
    )

SELECT lower(trim(address)) as address, trim(entity) as entity, trim(entity_unique_name) as entity_unique_name, trim(category) as category
FROM
(VALUES
('0xae7ab96520de3a18e5e111b5eaab095312d7fe84', 'Lido', 'Lido', 'Liquid Staking')
    , ('0xfddf38947afb03c621c71b06c9c70bce73f12999', 'Lido', 'Lido', 'Liquid Staking')
    , ('0xa76a7d0d06754e4fc4941519d1f9d56fd9f8d53b', 'Lido', 'Lido', 'Liquid Staking')
    , ('0x39dc6a99209b5e6b81dc8540c86ff10981ebda29', 'Staked.us', 'Staked.us', 'Staking Pools')
    , ('0x0194512e77d798e4871973d9cb9d7ddfc0ffd801', 'stakefish', 'stakefish 1', 'Staking Pools')
    , ('0xd4039ecc40aeda0582036437cf3ec02845da4c13', 'Kraken', 'Kraken 1', 'CEX')
    , ('0xa40dfee99e1c85dc97fdc594b16a460717838703', 'Kraken', 'Kraken 2', 'CEX')
    , ('0x622de9bb9ff8907414785a633097db438f9a2d86', 'Bitcoin Suisse', 'Bitcoin Suisse 1', 'Staking Pools')
    , ('0xdd9663bd979f1ab1bada85e1bc7d7f13cafe71f8', 'Bitcoin Suisse', 'Bitcoin Suisse 2', 'Staking Pools')
    , ('0xec70e3c8afe212039c3f6a2df1c798003bf7cfe9', 'Bitcoin Suisse', 'Bitcoin Suisse 3', 'Staking Pools')
    , ('0x3837ea2279b8e5c260a78f5f4181b783bbe76a8b', 'Bitcoin Suisse', 'Bitcoin Suisse 4', 'Staking Pools')
    , ('0x2a7077399b3e90f5392d55a1dc7046ad8d152348', 'Bitcoin Suisse', 'Bitcoin Suisse 5', 'Staking Pools')
    , ('0xc2288b408dc872a1546f13e6ebfa9c94998316a2', 'Bitcoin Suisse', 'Bitcoin Suisse 6', 'Staking Pools')
    , ('0xf2be95116845252a28bd43661651917dc183dab1', 'Figment', 'Figment 1', 'Staking Pools')
    , ('0x37ab162ab59e106d6072eb7a7bd4c4c2973455a7', 'Figment', 'Figment 2', 'Staking Pools')
    , ('0xb4e2e925d75793c33f5f94cd652f6c464665c76b', 'Figment', 'Figment 3', 'Staking Pools')
    , ('0xc874b064f465bdd6411d45734b56fac750cda29a', 'Stakewise', 'Stakewise', 'Liquid Staking')
    , ('0x84db6ee82b7cf3b47e8f19270abde5718b936670', 'Stkr (Ankr)', 'Stkr (Ankr)', 'Liquid Staking')
    , ('0x194bd70b59491ce1310ea0bceabdb6c23ac9d5b2', 'Huobi', 'Huobi 1', 'CEX')
    , ('0xb73f4d4e99f65ec4b16b684e44f81aeca5ba2b7c', 'Huobi', 'Huobi 2', 'CEX')
    , ('0xbf1556a7d625654e3d64d1f0466a60a697fac178', 'imToken', 'imToken Staking[NodeProviderInfStones]', 'Staking Pools')
    , ('0xbca3b7b87dcb15f0efa66136bc0e4684a3e5da4d', 'SharedStake', 'SharedStake', 'Liquid Staking')
    , ('0xeadcba8bf9aca93f627f31fb05470f5a0686ceca', 'StakeWise Solos', 'StakeWise Solos', 'Staking Pools')
    , ('0xfa5f9eaa65ffb2a75de092eb7f3fc84fc86b5b18', 'Abyss Finance', 'Abyss Finance', 'Batch Staking Contract')
    , ('0x66827bcd635f2bb1779d68c46aeb16541bca6ba8', 'PieDAO', 'PieDAO', 'Staking Pools')
    , ('0xd6216fc19db775df9774a6e33526131da7d19a2c', 'KuCoin', 'KuCoin 1', 'CEX')
    , ('0x1692e170361cefd1eb7240ec13d048fd9af6d667', 'KuCoin', 'KuCoin 2', 'CEX')
    , ('0x7b915c27a0ed48e2ce726ee40f20b2bf8a88a1b3', 'KuCoin', 'KuCoin 3', 'CEX')
    , ('0xcbc1065255cbc3ab41a6868c22d1f1c573ab89fd', 'CREAM', 'CREAM', 'Liquid Staking')
    , ('0x808e7133c700cf3a66e6a25aadb1fbef6be468b4', 'Bitstamp', 'Bitstamp', 'CEX')
    , ('0x12ec5befa9166fa327d4c345a93f0ac99dd2a7d8', 'Blox Staking', 'Blox Staking', 'Staking Pools')
    , ('0x24b2f1aeced4b34133152bb20cfd6a206a0ea33c', 'staked.finance', 'staked.finance', 'Liquid Staking')
    , ('0x0ca7b4b87feb2c0bda9cb37b6cd0de22b816cd04', 'MyColdWallet', 'MyColdWallet', 'Others')
    , ('0x1270a0aad453a315c5ab99397d88121c34453eb4', 'TideBit', 'TideBit', 'Others')
    , ('0x0038598ecb3b308ebc6c6e2c635bacaa3c5298a3', 'Poloniex', 'Poloniex', 'CEX')
    , ('0xd39aeeb73983e5fbc52b77a3957a48c1eeac8ed7', 'MintDice.com', 'MintDice.com', 'Others')
    , ('0x7ebf05749faf7eb78eff153e40c15890bb4578a4', 'neukind.com', 'neukind.com 1', 'Staking Pools')
    , ('0xa54be2edaa143e969a63fc744bbd2d511b50cbc3', 'neukind.com', 'neukind.com 2', 'Staking Pools')
    , ('0xac29ef7a7f4325ffa564de9abf67e5ace46c88f8', 'neukind.com', 'neukind.com 3', 'Staking Pools')
    , ('0xc3003f8b89f35a7bf3cb3a6ec3d8e4c3c8ce7cce', 'neukind.com', 'neukind.com 4', 'Staking Pools')
    , ('0x8e1d8b147cc4c939a597dc501c47cc8b4ab26bd5', 'Tetranode', 'Tetranode', 'Independent Staker')
    , ('0x1db3439a222c519ab44bb1144fc28167b4fa6ee6', 'Vitalik Buterin', 'Vitalik Buterin', 'Independent Staker')
    , ('0x49df3cca2670eb0d591146b16359fe336e476f29', 'stereum.net', 'stereum.net 1', 'Others')
    , ('0x62dfeb55fcbdcb921446168eecfd1406379a1ee1', 'stereum.net', 'stereum.net 2', 'Others')
    , ('0x2be0282532ad9fa7cc4c45aeaa1707d2e93357c2', 'Blockdaemon.com', 'Blockdaemon.com', 'Others')
    , ('0x5e59aab1f114234f003008300c3d7593c6ceea26', 'boxfish.studio', 'boxfish.studio', 'Others')
    , ('0xd1933df1c223ad7cb5716b066ca26bc24569e622', 'Ethereum on ARM', 'Ethereum on ARM', 'Others')
    , ('0x5a0036bcab4501e70f086c634e2958a8beae3a11', 'OKX', 'OKX', 'CEX')
    , ('0x607ebc82329d0cac3027b83d15e4b4e816f131b7', 'StakeHound', 'StakeHound', 'Liquid Staking')
    , ('0xc236c3ec83351b07f148afbadc252cce2c07972e', 'Bitfinex', 'Bitfinex 1', 'CEX')
    , ('0xe733455faddf4999176e99a0ec084e978f5552ed', 'Bitfinex', 'Bitfinex 2', 'CEX')
    , ('0x4c2f150fc90fed3d8281114c2349f1906cde5346', 'Gemini', 'Gemini', 'CEX')
    , ('0xbb84d966c09264ce9a2104a4a20bb378369986db', 'WEX Exchange', 'WEX Exchange', 'CEX')
    , ('0xbafa44efe7901e04e39dad13167d089c559c1138', 'Frax Finance', 'Frax Finance', 'Liquid Staking')
    --, ('0xefe9a82d56cd965d7b332c7ac1feb15c53cd4340', 'stakefish', 'stakefish 2', 'Staking Pools') -- fork of stakefish batch deposit contract that isn't controlled by stakefish
    , ('0xeee27662c2b8eba3cd936a23f039f3189633e4c8', 'Celsius', 'Celsius', 'Staking Pools')
    , ('0xe0c8df4270f4342132ec333f6048cb703e7a9c77', 'Swell', 'Swell', 'Liquid Staking')
    , ('0xb3d9cf8e163bbc840195a97e81f8a34e295b8f39', 'Swell', 'Swell', 'Liquid Staking')
    , ('0x5180db0237291a6449dda9ed33ad90a38787621c', 'Frax Finance', 'Frax Finance Investor Custodian', 'Liquid Staking')
    , ('0xaab27b150451726ec7738aa1d0a94505c8729bd1', 'Eden Network', 'Eden Network', 'Others')
    , ('0x234ee9e35f8e9749a002fc42970d570db716453b', 'Gate.io', 'Gate.io', 'CEX')
    , ('0x6c7c332a090c8d2085857cf3220ea01c6d45a723', 'Unagii', 'Unagii', 'Staking Pools')
    , ('0x663d3947f03ef5b387992b880ac85940057c13e3', 'WeekInEth', 'WeekInEth', 'Others')
    , ('0x3ccc0b321ec18997490c8bfc2c882ef83d546ddd', 'Cake DeFi', 'Cake DeFi', 'Staking Pools')
    , ('0x31e180e06d771dbafa3d6eea452195ad1020fbdb', 'Ethereum Hive', 'Ethereum Hive', 'Staking Pools')
    , ('0x6b523cd4fcdf3332bcb3177050e22cf7272b4c3a', 'Consensus Cell Network', 'Consensus Cell Network', 'Others')
    , ('0xd3b16f647ad234f8b5bb2bdbe8e919daa5268681', 'FOAM Signal', 'FOAM Signal', 'Others')
    , ('0x3187a42658417a4d60866163a4534ce00d40c0c8', 'ssv.network', 'ssv.network', 'Liquid Staking')
    , ('0xea6b7151b138c274ed8d4d61328352545ef2d4b7', 'Harbour', 'Harbour', 'Liquid Staking')
    , ('0x588e859cb38fecf2d56925c0512471ab47aa9ff1', 'StaFi', 'StaFi SuperNode', 'Liquid Staking')
    , ('0x1c906685384df71e3fafa6f3b21bd884e9d44f4b', 'StaFi', 'StaFi LightNode', 'Liquid Staking')
    , ('0xa8582b5a0f615bc21d7780618557042be60b32ed', 'Bitpie', 'Bitpie', 'Staking Pools')
    , ('0xec1d6163e05b3f5d0fb8f354881f6c8b793ad612', 'Bifrost', 'Bifrost', 'Liquid Staking')
    , ('0xf79caa45612fb183c4e258ed449bfa632d7400b9', 'Everstake Pool', 'Everstake', 'Staking Pools')
    , ('0xcf07df57a6b338a20d50114a79fee09d28b13d72', 'cryptostake.com', 'cryptostake.com', 'Staking Pools')
    , ('0x2915f91dcff0be7b60df411f164827d517caca67', 'TokenPocket', 'TokenPocket', 'Others')
    , ('0xfcd50905214325355a57ae9df084c5dd40d5d478', 'Sigma Prime Team', 'Sigma Prime Team', 'Independent Staker')
    , ('0x7badde47f41ceb2c0889091c8fc69e4d9059fb19', 'Prysm Team', 'Prysm Team', 'Independent Staker')
    , ('0x43a0927a6361258e6cbaed415df275a412c543b5', 'Teku Team', 'Teku Team', 'Independent Staker')
    , ('0x5efaefd5f8a42723bb095194c9202bf2b83d8eb6', 'Nimbus Team', 'Nimbus Team', 'Independent Staker')
    , ('0x4ca21e4d3a86e7399698f88686f5596dbe74adeb', 'P2P.org', 'P2P.org', 'Staking Pools')
    , ('0x8103151e2377e78c04a3d2564e20542680ed3096', 'Node DAO', 'Node DAO', 'Liquid Staking')
    , ('0x4befa2aa9c305238aa3e0b5d17eb20c045269e9d', 'RockX', 'RockX', 'Staking Pools')
    , ('0xea674fdde714fd979de3edf0f56aa9716b898ec8', 'Ethermine', 'Ethermine', 'Staking Pools')
    ) 
    x (address, entity, entity_unique_name, category)

    UNION ALL

    SELECT coinbase.address
    , 'Coinbase' AS name
    , 'Coinbase ' || ROW_NUMBER() OVER (ORDER BY MIN(coinbase.block_time)) AS entity_unique_name
    , 'CEX' AS category
    FROM (
            SELECT
                et.from AS address
                , et.block_time
            FROM {{ source('ethereum', 'traces') }} et
            INNER JOIN {{ source('ethereum', 'traces') }} et2 ON et2.from=et.from
                AND et2.to IN (SELECT address FROM {{ ref('cex_ethereum_addresses') }} WHERE cex_name = 'Coinbase')
                {% if not is_incremental() %}
                AND et2.block_time >= '2020-10-14'
                {% endif %}
                {% if is_incremental() %}
                AND et2.block_time >= date_trunc("day", now() - interval '1 week')
                {% endif %}
            WHERE et.to='0x00000000219ab540356cbb839cbe05303d7705fa'
                AND et.success
                {% if not is_incremental() %}
                AND et.block_time >= '2020-10-14'
                {% endif %}
                {% if is_incremental() %}
                AND et.block_time >= date_trunc("day", now() - interval '1 week')
                {% endif %}
            GROUP BY et.from, et.block_time
        ) coinbase
    GROUP BY coinbase.address

    UNION ALL

    SELECT binance.address
    , 'Binance' AS name
    , 'Binance ' || ROW_NUMBER() OVER (ORDER BY MIN(t.block_time)) AS entity_unique_name
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
            {% if not is_incremental() %}
            AND block_time >= '2020-10-14'
            {% endif %}
            {% if is_incremental() %}
            AND block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
        GROUP BY to
    ) binance
    INNER JOIN {{ source('ethereum', 'traces') }} t
        ON binance.address=t.from
        AND t.to='0x00000000219ab540356cbb839cbe05303d7705fa'
        {% if not is_incremental() %}
        AND t.block_time >= '2020-10-14'
        {% endif %}
        {% if is_incremental() %}
        AND t.block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    GROUP BY binance.address

    UNION ALL
    
    SELECT address
    , entity AS name
    , entity || ' ' || ROW_NUMBER() OVER (PARTITION BY entity ORDER BY first_used) AS entity_unique_name
    , category AS category
    FROM (
        SELECT traces.from AS address
        , c.entity
        , c.category
        , MIN(txs.block_time) AS first_used
        FROM {{ source('ethereum', 'transactions') }} txs
        INNER JOIN {{ source('ethereum', 'traces') }} traces
            ON txs.hash=traces.tx_hash 
            AND traces.to='0x00000000219ab540356cbb839cbe05303d7705fa'
            {% if not is_incremental() %}
            AND traces.block_time >= '2020-10-14'
            {% endif %}
            {% if is_incremental() %}
            AND traces.block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
        INNER JOIN contracts c ON c.address=txs.to
        WHERE txs.to IN (SELECT address FROM contracts)
            {% if not is_incremental() %}
            AND txs.block_time >= '2020-10-14'
            {% endif %}
            {% if is_incremental() %}
            AND txs.block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
        GROUP BY 1, 2, 3
        )
