{{ config(
        schema = 'tornado_cash_ethereum',
        alias = alias('withdrawals'),
        materialized='incremental',
        partition_by=['block_date'],
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['block_date', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "tornado_cash",
                                    \'["hildobby", "dot2dotseurat"]\') }}'
        )
}}

{% set ethereum_start_date = '2019-12-16' %}
{% set eth_erc20_pt1_start_date = '2019-12-16' %}
{% set eth_erc20_pt2_start_date = '2021-04-02' %}

-- Ethereum (ETH)
SELECT tc.evt_block_time AS block_time
, '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' AS currency_contract
, 'ETH' AS currency_symbol
, 'ethereum' AS blockchain
, 'classic' AS tornado_version
, et.from AS tx_from
, tc.nullifierHash AS nullifier
, tc.fee/POWER(10, 18) AS fee
, tc.relayer
, tc.to AS recipient
, tc.contract_address AS contract_address
, CASE WHEN tc.contract_address='0x12d66f87a04a9e220743712ce6d9bb1b5616b8fc' THEN 0.1
        WHEN tc.contract_address='0x47ce0c6ed5b0ce3d3a51fdb1c52dc66a7c3c2936' THEN 1
        WHEN tc.contract_address='0x910cbd523d972eb0a6f4cae4618ad62622b39dbf' THEN 10
        WHEN tc.contract_address='0xa160cdab225685da1d56aa342ad8841c3b53f291' THEN 100
        END AS amount
, tc.evt_tx_hash AS tx_hash
, tc.evt_index
, TRY_CAST(date_trunc('DAY', tc.evt_block_time) AS date) AS block_date
FROM {{ source('tornado_cash_ethereum','eth_evt_Withdrawal') }} tc
INNER JOIN {{ source('ethereum','transactions') }} et
        ON et.hash=tc.evt_tx_hash
        {% if not is_incremental() %}
        AND et.block_time >= '{{ethereum_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        AND et.block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
{% if not is_incremental() %}
WHERE tc.evt_block_time >= '{{ethereum_start_date}}'
{% endif %}
{% if is_incremental() %}
WHERE tc.evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}

UNION

-- Ethereum (ERC20s Part 1)
SELECT tc.evt_block_time AS block_time
, CASE WHEN tc.contract_address='0xd4b88df4d29f5cedd6857912842cff3b20c8cfa3' THEN '0x6b175474e89094c44da98b954eedeac495271d0f'
        WHEN tc.contract_address='0xfd8610d20aa15b7b2e3be39b396a1bc3516c7144' THEN '0x6b175474e89094c44da98b954eedeac495271d0f'
        WHEN tc.contract_address='0xf60dd140cff0706bae9cd734ac3ae76ad9ebc32a' THEN '0x6b175474e89094c44da98b954eedeac495271d0f'
        WHEN tc.contract_address='0x07687e702b410fa43f4cb4af7fa097918ffd2730' THEN '0x6b175474e89094c44da98b954eedeac495271d0f'
        WHEN tc.contract_address='0x23773e65ed146a459791799d01336db287f25334' THEN '0x6b175474e89094c44da98b954eedeac495271d0f'
        WHEN tc.contract_address='0xd96f2b1c14db8458374d9aca76e26c3d18364307' THEN '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
        WHEN tc.contract_address='0x4736dcf1b7a3d580672cce6e7c65cd5cc9cfba9d' THEN '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
        WHEN tc.contract_address='0xd691f27f38b395864ea86cfc7253969b409c362d' THEN '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
        WHEN tc.contract_address='0x169ad27a470d064dede56a2d3ff727986b15d52b' THEN '0xdac17f958d2ee523a2206206994597c13d831ec7'
        WHEN tc.contract_address='0x0836222f2b2b24a3f36f98668ed8f0b38d1a872f' THEN '0xdac17f958d2ee523a2206206994597c13d831ec7'
        WHEN tc.contract_address='0xf67721a2d8f736e75a49fdd7fad2e31d8676542a' THEN '0xdac17f958d2ee523a2206206994597c13d831ec7'
        WHEN tc.contract_address='0x9ad122c22b14202b4490edaf288fdb3c7cb3ff5e' THEN '0xdac17f958d2ee523a2206206994597c13d831ec7'
        WHEN tc.contract_address='0x22aaa7720ddd5388a3c0a3333430953c68f1849b' THEN '0x5d3a536e4d6dbd6114cc1ead35777bab948e3643'
        WHEN tc.contract_address='0xba214c1c1928a32bffe790263e38b4af9bfcd659' THEN '0x5d3a536e4d6dbd6114cc1ead35777bab948e3643'
        WHEN tc.contract_address='0xb1c8094b234dce6e03f10a5b673c1d8c69739a00' THEN '0x5d3a536e4d6dbd6114cc1ead35777bab948e3643'
        WHEN tc.contract_address='0x2717c5e28cf931547b621a5dddb772ab6a35b701' THEN '0x5d3a536e4d6dbd6114cc1ead35777bab948e3643'
        WHEN tc.contract_address='0xaeaac358560e11f52454d997aaff2c5731b6f8a6' THEN '0x39aa39c021dfbae8fac545936693ac917d5e7563'
        WHEN tc.contract_address='0x1356c899d8c9467c7f71c195612f8a395abf2f0a' THEN '0x39aa39c021dfbae8fac545936693ac917d5e7563'
        WHEN tc.contract_address='0xa60c772958a3ed56c1f15dd055ba37ac8e523a0d' THEN '0x39aa39c021dfbae8fac545936693ac917d5e7563'
        WHEN tc.contract_address='0x178169b423a011fff22b9e3f3abea13414ddd0f1' THEN '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599'
        WHEN tc.contract_address='0x610b717796ad172b316836ac95a2ffad065ceab4' THEN '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599'
        WHEN tc.contract_address='0xbb93e510bbcd0b7beb5a853875f9ec60275cf498' THEN '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599'
        END AS currency_contract
, CASE WHEN tc.contract_address='0xd4b88df4d29f5cedd6857912842cff3b20c8cfa3' THEN 'DAI'
        WHEN tc.contract_address='0xfd8610d20aa15b7b2e3be39b396a1bc3516c7144' THEN 'DAI'
        WHEN tc.contract_address='0xf60dd140cff0706bae9cd734ac3ae76ad9ebc32a' THEN 'DAI'
        WHEN tc.contract_address='0x07687e702b410fa43f4cb4af7fa097918ffd2730' THEN 'DAI'
        WHEN tc.contract_address='0x23773e65ed146a459791799d01336db287f25334' THEN 'DAI'
        WHEN tc.contract_address='0xd96f2b1c14db8458374d9aca76e26c3d18364307' THEN 'USDC'
        WHEN tc.contract_address='0x4736dcf1b7a3d580672cce6e7c65cd5cc9cfba9d' THEN 'USDC'
        WHEN tc.contract_address='0xd691f27f38b395864ea86cfc7253969b409c362d' THEN 'USDC'
        WHEN tc.contract_address='0x169ad27a470d064dede56a2d3ff727986b15d52b' THEN 'USDT'
        WHEN tc.contract_address='0x0836222f2b2b24a3f36f98668ed8f0b38d1a872f' THEN 'USDT'
        WHEN tc.contract_address='0xf67721a2d8f736e75a49fdd7fad2e31d8676542a' THEN 'USDT'
        WHEN tc.contract_address='0x9ad122c22b14202b4490edaf288fdb3c7cb3ff5e' THEN 'USDT'
        WHEN tc.contract_address='0x22aaa7720ddd5388a3c0a3333430953c68f1849b' THEN 'cDAI'
        WHEN tc.contract_address='0xba214c1c1928a32bffe790263e38b4af9bfcd659' THEN 'cDAI'
        WHEN tc.contract_address='0xb1c8094b234dce6e03f10a5b673c1d8c69739a00' THEN 'cDAI'
        WHEN tc.contract_address='0x2717c5e28cf931547b621a5dddb772ab6a35b701' THEN 'cDAI'
        WHEN tc.contract_address='0xaeaac358560e11f52454d997aaff2c5731b6f8a6' THEN 'cUSDC'
        WHEN tc.contract_address='0x1356c899d8c9467c7f71c195612f8a395abf2f0a' THEN 'cUSDC'
        WHEN tc.contract_address='0xa60c772958a3ed56c1f15dd055ba37ac8e523a0d' THEN 'cUSDC'
        WHEN tc.contract_address='0x178169b423a011fff22b9e3f3abea13414ddd0f1' THEN 'WBTC'
        WHEN tc.contract_address='0x610b717796ad172b316836ac95a2ffad065ceab4' THEN 'WBTC'
        WHEN tc.contract_address='0xbb93e510bbcd0b7beb5a853875f9ec60275cf498' THEN 'WBTC'
        END AS currency_symbol
, 'ethereum' AS blockchain
, 'classic' AS tornado_version
, et.from AS tx_from
, tc.nullifierHash AS nullifier
, tc.fee/POWER(10, 18) AS fee
, tc.relayer
, tc.to AS recipient
, tc.contract_address AS contract_address
, CASE WHEN tc.contract_address='0xd4b88df4d29f5cedd6857912842cff3b20c8cfa3' THEN 100
        WHEN tc.contract_address='0xfd8610d20aa15b7b2e3be39b396a1bc3516c7144' THEN 1000
        WHEN tc.contract_address='0xf60dd140cff0706bae9cd734ac3ae76ad9ebc32a' THEN 1000
        WHEN tc.contract_address='0x07687e702b410fa43f4cb4af7fa097918ffd2730' THEN 10000
        WHEN tc.contract_address='0x23773e65ed146a459791799d01336db287f25334' THEN 100000
        WHEN tc.contract_address='0xd96f2b1c14db8458374d9aca76e26c3d18364307' THEN 100
        WHEN tc.contract_address='0x4736dcf1b7a3d580672cce6e7c65cd5cc9cfba9d' THEN 1000
        WHEN tc.contract_address='0xd691f27f38b395864ea86cfc7253969b409c362d' THEN 10000
        WHEN tc.contract_address='0x169ad27a470d064dede56a2d3ff727986b15d52b' THEN 100
        WHEN tc.contract_address='0x0836222f2b2b24a3f36f98668ed8f0b38d1a872f' THEN 1000
        WHEN tc.contract_address='0xf67721a2d8f736e75a49fdd7fad2e31d8676542a' THEN 10000
        WHEN tc.contract_address='0x9ad122c22b14202b4490edaf288fdb3c7cb3ff5e' THEN 100000
        WHEN tc.contract_address='0x22aaa7720ddd5388a3c0a3333430953c68f1849b' THEN 5000
        WHEN tc.contract_address='0xba214c1c1928a32bffe790263e38b4af9bfcd659' THEN 50000
        WHEN tc.contract_address='0xb1c8094b234dce6e03f10a5b673c1d8c69739a00' THEN 500000
        WHEN tc.contract_address='0x2717c5e28cf931547b621a5dddb772ab6a35b701' THEN 500000
        WHEN tc.contract_address='0xaeaac358560e11f52454d997aaff2c5731b6f8a6' THEN 5000
        WHEN tc.contract_address='0x1356c899d8c9467c7f71c195612f8a395abf2f0a' THEN 50000
        WHEN tc.contract_address='0xa60c772958a3ed56c1f15dd055ba37ac8e523a0d' THEN 500000
        WHEN tc.contract_address='0x178169b423a011fff22b9e3f3abea13414ddd0f1' THEN 0.1
        WHEN tc.contract_address='0x610b717796ad172b316836ac95a2ffad065ceab4' THEN 1
        WHEN tc.contract_address='0xbb93e510bbcd0b7beb5a853875f9ec60275cf498' THEN 10
        END AS amount
, tc.evt_tx_hash AS tx_hash
, tc.evt_index
, TRY_CAST(date_trunc('DAY', tc.evt_block_time) AS date) AS block_date
FROM {{ source('tornado_cash_ethereum','erc20_evt_Withdrawal') }} tc
INNER JOIN {{ source('ethereum','transactions') }} et
        ON et.hash=tc.evt_tx_hash
        {% if not is_incremental() %}
        AND et.block_time >= '{{eth_erc20_pt1_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        AND et.block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
{% if not is_incremental() %}
WHERE tc.evt_block_time >= '{{eth_erc20_pt1_start_date}}'
{% endif %}
{% if is_incremental() %}
WHERE tc.evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}

UNION

-- Ethereum (ERC20s Part 2)
SELECT tc.evt_block_time AS block_time
, CASE WHEN tc.contract_address='0xd4b88df4d29f5cedd6857912842cff3b20c8cfa3' THEN '0x6b175474e89094c44da98b954eedeac495271d0f'
        WHEN tc.contract_address='0xfd8610d20aa15b7b2e3be39b396a1bc3516c7144' THEN '0x6b175474e89094c44da98b954eedeac495271d0f'
        WHEN tc.contract_address='0xf60dd140cff0706bae9cd734ac3ae76ad9ebc32a' THEN '0x6b175474e89094c44da98b954eedeac495271d0f'
        WHEN tc.contract_address='0x07687e702b410fa43f4cb4af7fa097918ffd2730' THEN '0x6b175474e89094c44da98b954eedeac495271d0f'
        WHEN tc.contract_address='0x23773e65ed146a459791799d01336db287f25334' THEN '0x6b175474e89094c44da98b954eedeac495271d0f'
        WHEN tc.contract_address='0xd96f2b1c14db8458374d9aca76e26c3d18364307' THEN '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
        WHEN tc.contract_address='0x4736dcf1b7a3d580672cce6e7c65cd5cc9cfba9d' THEN '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
        WHEN tc.contract_address='0xd691f27f38b395864ea86cfc7253969b409c362d' THEN '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
        WHEN tc.contract_address='0x169ad27a470d064dede56a2d3ff727986b15d52b' THEN '0xdac17f958d2ee523a2206206994597c13d831ec7'
        WHEN tc.contract_address='0x0836222f2b2b24a3f36f98668ed8f0b38d1a872f' THEN '0xdac17f958d2ee523a2206206994597c13d831ec7'
        WHEN tc.contract_address='0xf67721a2d8f736e75a49fdd7fad2e31d8676542a' THEN '0xdac17f958d2ee523a2206206994597c13d831ec7'
        WHEN tc.contract_address='0x9ad122c22b14202b4490edaf288fdb3c7cb3ff5e' THEN '0xdac17f958d2ee523a2206206994597c13d831ec7'
        WHEN tc.contract_address='0x22aaa7720ddd5388a3c0a3333430953c68f1849b' THEN '0x5d3a536e4d6dbd6114cc1ead35777bab948e3643'
        WHEN tc.contract_address='0xba214c1c1928a32bffe790263e38b4af9bfcd659' THEN '0x5d3a536e4d6dbd6114cc1ead35777bab948e3643'
        WHEN tc.contract_address='0xb1c8094b234dce6e03f10a5b673c1d8c69739a00' THEN '0x5d3a536e4d6dbd6114cc1ead35777bab948e3643'
        WHEN tc.contract_address='0x2717c5e28cf931547b621a5dddb772ab6a35b701' THEN '0x5d3a536e4d6dbd6114cc1ead35777bab948e3643'
        WHEN tc.contract_address='0xaeaac358560e11f52454d997aaff2c5731b6f8a6' THEN '0x39aa39c021dfbae8fac545936693ac917d5e7563'
        WHEN tc.contract_address='0x1356c899d8c9467c7f71c195612f8a395abf2f0a' THEN '0x39aa39c021dfbae8fac545936693ac917d5e7563'
        WHEN tc.contract_address='0xa60c772958a3ed56c1f15dd055ba37ac8e523a0d' THEN '0x39aa39c021dfbae8fac545936693ac917d5e7563'
        WHEN tc.contract_address='0x178169b423a011fff22b9e3f3abea13414ddd0f1' THEN '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599'
        WHEN tc.contract_address='0x610b717796ad172b316836ac95a2ffad065ceab4' THEN '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599'
        WHEN tc.contract_address='0xbb93e510bbcd0b7beb5a853875f9ec60275cf498' THEN '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599'
        END AS currency_contract
, CASE WHEN tc.contract_address='0xd4b88df4d29f5cedd6857912842cff3b20c8cfa3' THEN 'DAI'
        WHEN tc.contract_address='0xfd8610d20aa15b7b2e3be39b396a1bc3516c7144' THEN 'DAI'
        WHEN tc.contract_address='0xf60dd140cff0706bae9cd734ac3ae76ad9ebc32a' THEN 'DAI'
        WHEN tc.contract_address='0x07687e702b410fa43f4cb4af7fa097918ffd2730' THEN 'DAI'
        WHEN tc.contract_address='0x23773e65ed146a459791799d01336db287f25334' THEN 'DAI'
        WHEN tc.contract_address='0xd96f2b1c14db8458374d9aca76e26c3d18364307' THEN 'USDC'
        WHEN tc.contract_address='0x4736dcf1b7a3d580672cce6e7c65cd5cc9cfba9d' THEN 'USDC'
        WHEN tc.contract_address='0xd691f27f38b395864ea86cfc7253969b409c362d' THEN 'USDC'
        WHEN tc.contract_address='0x169ad27a470d064dede56a2d3ff727986b15d52b' THEN 'USDT'
        WHEN tc.contract_address='0x0836222f2b2b24a3f36f98668ed8f0b38d1a872f' THEN 'USDT'
        WHEN tc.contract_address='0xf67721a2d8f736e75a49fdd7fad2e31d8676542a' THEN 'USDT'
        WHEN tc.contract_address='0x9ad122c22b14202b4490edaf288fdb3c7cb3ff5e' THEN 'USDT'
        WHEN tc.contract_address='0x22aaa7720ddd5388a3c0a3333430953c68f1849b' THEN 'cDAI'
        WHEN tc.contract_address='0xba214c1c1928a32bffe790263e38b4af9bfcd659' THEN 'cDAI'
        WHEN tc.contract_address='0xb1c8094b234dce6e03f10a5b673c1d8c69739a00' THEN 'cDAI'
        WHEN tc.contract_address='0x2717c5e28cf931547b621a5dddb772ab6a35b701' THEN 'cDAI'
        WHEN tc.contract_address='0xaeaac358560e11f52454d997aaff2c5731b6f8a6' THEN 'cUSDC'
        WHEN tc.contract_address='0x1356c899d8c9467c7f71c195612f8a395abf2f0a' THEN 'cUSDC'
        WHEN tc.contract_address='0xa60c772958a3ed56c1f15dd055ba37ac8e523a0d' THEN 'cUSDC'
        WHEN tc.contract_address='0x178169b423a011fff22b9e3f3abea13414ddd0f1' THEN 'WBTC'
        WHEN tc.contract_address='0x610b717796ad172b316836ac95a2ffad065ceab4' THEN 'WBTC'
        WHEN tc.contract_address='0xbb93e510bbcd0b7beb5a853875f9ec60275cf498' THEN 'WBTC'
        END AS currency_symbol
, 'ethereum' AS blockchain
, 'classic' AS tornado_version
, et.from AS tx_from
, tc.nullifierHash AS nullifier
, tc.fee/POWER(10, 18) AS fee
, tc.relayer
, tc.to AS recipient
, tc.contract_address AS contract_address
, CASE WHEN tc.contract_address='0xd4b88df4d29f5cedd6857912842cff3b20c8cfa3' THEN 100
        WHEN tc.contract_address='0xfd8610d20aa15b7b2e3be39b396a1bc3516c7144' THEN 1000
        WHEN tc.contract_address='0xf60dd140cff0706bae9cd734ac3ae76ad9ebc32a' THEN 1000
        WHEN tc.contract_address='0x07687e702b410fa43f4cb4af7fa097918ffd2730' THEN 10000
        WHEN tc.contract_address='0x23773e65ed146a459791799d01336db287f25334' THEN 100000
        WHEN tc.contract_address='0xd96f2b1c14db8458374d9aca76e26c3d18364307' THEN 100
        WHEN tc.contract_address='0x4736dcf1b7a3d580672cce6e7c65cd5cc9cfba9d' THEN 1000
        WHEN tc.contract_address='0xd691f27f38b395864ea86cfc7253969b409c362d' THEN 10000
        WHEN tc.contract_address='0x169ad27a470d064dede56a2d3ff727986b15d52b' THEN 100
        WHEN tc.contract_address='0x0836222f2b2b24a3f36f98668ed8f0b38d1a872f' THEN 1000
        WHEN tc.contract_address='0xf67721a2d8f736e75a49fdd7fad2e31d8676542a' THEN 10000
        WHEN tc.contract_address='0x9ad122c22b14202b4490edaf288fdb3c7cb3ff5e' THEN 100000
        WHEN tc.contract_address='0x22aaa7720ddd5388a3c0a3333430953c68f1849b' THEN 5000
        WHEN tc.contract_address='0xba214c1c1928a32bffe790263e38b4af9bfcd659' THEN 50000
        WHEN tc.contract_address='0xb1c8094b234dce6e03f10a5b673c1d8c69739a00' THEN 500000
        WHEN tc.contract_address='0x2717c5e28cf931547b621a5dddb772ab6a35b701' THEN 500000
        WHEN tc.contract_address='0xaeaac358560e11f52454d997aaff2c5731b6f8a6' THEN 5000
        WHEN tc.contract_address='0x1356c899d8c9467c7f71c195612f8a395abf2f0a' THEN 50000
        WHEN tc.contract_address='0xa60c772958a3ed56c1f15dd055ba37ac8e523a0d' THEN 500000
        WHEN tc.contract_address='0x178169b423a011fff22b9e3f3abea13414ddd0f1' THEN 0.1
        WHEN tc.contract_address='0x610b717796ad172b316836ac95a2ffad065ceab4' THEN 1
        WHEN tc.contract_address='0xbb93e510bbcd0b7beb5a853875f9ec60275cf498' THEN 10
        END AS amount
, tc.evt_tx_hash AS tx_hash
, tc.evt_index
, TRY_CAST(date_trunc('DAY', tc.evt_block_time) AS date) AS block_date
FROM {{ source('tornado_cash_ethereum','ERC20Tornado_evt_Withdrawal') }} tc
INNER JOIN {{ source('ethereum','transactions') }} et
        ON et.hash=tc.evt_tx_hash
        {% if not is_incremental() %}
        AND et.block_time >= '{{eth_erc20_pt2_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        AND et.block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
{% if not is_incremental() %}
WHERE tc.evt_block_time >= '{{eth_erc20_pt2_start_date}}'
{% endif %}
{% if is_incremental() %}
WHERE tc.evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}