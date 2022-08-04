{{ config(
        alias ='events',
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key='unique_trade_id'
        )
}}

WITH
-- #################################################################
-- Vaults Data
-- #################################################################
vaults_t0 AS (select * from nftx_v2."NFTXVaultFactoryUpgradeable_v1_evt_NewVault" UNION select * from nftx_v2."NFTXVaultFactoryUpgradeable_v1_evt_NewVault0"),
vaults_t1 AS (select DISTINCT ON ("vaultId") * from vaults_t0 ORDER  BY "vaultId", evt_block_time DESC),
vaults_t2 AS (select * from nftx_v2."NFTXVaultFactoryUpgradeable_v1_call_createVault" UNION select * from nftx_v2."NFTXVaultFactoryUpgradeable_v1_call_createVault0"),
vaults_t3 AS (select "vaultId","vaultAddress","assetAddress",name,symbol,call_block_time from vaults_t1 LEFT JOIN vaults_t2 ON vaults_t1."vaultId" = vaults_t2."output_0"),
vaults_t4 AS (select DISTINCT ON ("vaultId") "vaultId","vaultAddress","assetAddress",name,symbol from vaults_t3 ORDER  BY "vaultId", call_block_time DESC),
pools AS (select output_0 as pool, "_vaultToken" as vault, call_block_time from nftx_v2."StakingTokenProvider_call_stakingTokenForVaultToken" where call_success = true),
vaults_t5 AS (select "vaultId","vaultAddress","assetAddress",name,symbol,call_block_time,pool from vaults_t4 LEFT JOIN pools ON vaults_t4."vaultAddress" = pools."vault"),
vaults_t6 AS (select DISTINCT ON ("vaultId") "vaultId","vaultAddress","assetAddress",name,symbol,pool from vaults_t5 ORDER  BY "vaultId", call_block_time DESC),
-- #################################################################
-- #################################################################


-- #################################################################
-- Vault Prices
-- #################################################################
trades AS (SELECT * FROM dex.trades,vaults_t6 WHERE project = 'Sushiswap' AND exchange_contract_address IN (pool)),
trades_2 AS
(
    SELECT *,
    CASE 
        WHEN tr.token_b_address = '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' THEN tr.token_a_amount_raw
        ELSE tr.token_b_amount_raw
    END AS token_amt,
    CASE 
        WHEN tr.token_b_address = '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' THEN tr.token_b_amount_raw
    ELSE tr.token_a_amount_raw
    END AS eth_amt
    FROM trades tr
),
tok_price_0 AS (select *, eth_amt/token_amt as tok_price, (usd_amount / (token_amt/10^18)) as tok_price_usd from trades_2),
tok_price AS (select DISTINCT ON ("pool") "tok_price","tok_price_usd","pool" from tok_price_0 ORDER  BY "pool", block_time DESC),
eth_price AS (select price as eth_price from prices.usd where symbol = 'WETH' order by minute desc limit 1),
vaults AS
(
    select "vaultId","vaultAddress","assetAddress",name,symbol,vaults_t6.pool,
        COALESCE (tok_price , 0 ) as tok_price,
        COALESCE (tok_price_usd , 0 ) as tok_price_usd
    from vaults_t6 LEFT JOIN tok_price ON vaults_t6."pool" = tok_price."pool"
),
-- #################################################################
-- #################################################################


-- #################################################################
-- Vault Txs
-- #################################################################
Minted_t1 AS
(
    select * from nftx_v2."NFTXVaultUpgradeable_v1_evt_Minted"
    where "to" NOT IN ('\x0B8Ee2Ee7d6f3bFB73C9aE2127558D1172B65fb1','\xaf93fcce0548d3124a5fc3045adaf1dde4e8bf7e','\x28E9162fC10Bb3A7f98f44e90fA7273698fce360')
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    AND evt_block_time >= (select max(block_time) from {{ this }})
    {% endif %}
    UNION
    select * from nftx_v2."NFTXVaultUpgradeable_v1_evt_Minted0"
    where "to" NOT IN ('\x0B8Ee2Ee7d6f3bFB73C9aE2127558D1172B65fb1','\xaf93fcce0548d3124a5fc3045adaf1dde4e8bf7e','\x28E9162fC10Bb3A7f98f44e90fA7273698fce360')
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    AND evt_block_time >= (select max(block_time) from {{ this }})
    {% endif %}
),
Minted_t2 AS
(
    select
        evt_block_time as time,
        evt_block_number,
        evt_tx_hash as tx,
        evt_index,
        'Mint' as type,
        'Sell' as tx_type,
        contract_address as vault,
        nftidx as nfts,
        ordinality as trade_id,
        "to" as seller,
        contract_address as buyer
    from Minted_t1, unnest("nftIds") WITH ORDINALITY nftidx
    ORDER BY time
),
Redeemed_t1 AS
(
    select * from nftx_v2."NFTXVaultUpgradeable_v1_evt_Redeemed"
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where evt_block_time >= (select max(block_time) from {{ this }})
    {% endif %}
    UNION
    select * from nftx_v2."NFTXVaultUpgradeable_v1_evt_Redeemed0"
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where evt_block_time >= (select max(block_time) from {{ this }})
    {% endif %}
),
Redeemed_t2 AS
(
    select
    evt_block_time as time,
    evt_block_number,
    evt_tx_hash as tx,
    evt_index,
    'Redeem' as type,
    'Buy' as tx_type,
    contract_address as vault,
    nftidx as nfts,
    ordinality as trade_id,
    contract_address as seller,
    "to" as buyer
    from
    Redeemed_t1, unnest("nftIds") WITH ORDINALITY nftidx
    ORDER BY time
),
Swapped_t1 AS
(
    select * from nftx_v2."NFTXVaultUpgradeable_v1_evt_Swapped"
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where evt_block_time >= (select max(block_time) from {{ this }})
    {% endif %}
    UNION
    select * from nftx_v2."NFTXVaultUpgradeable_v1_evt_Swapped0"
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where evt_block_time >= (select max(block_time) from {{ this }})
    {% endif %}
),
Swapped_t2M AS
(
    select
    evt_block_time as time,
    evt_block_number,
    evt_tx_hash as tx,
    evt_index,
    'Swap' as type,
    'Sell' as tx_type,
    contract_address as vault,
    nftidx as nfts,
    ordinality as trade_id,
    "to" as seller,
    contract_address as buyer
    from
    Swapped_t1, unnest("nftIds") WITH ORDINALITY nftidx
    ORDER BY time
),
Swapped_t2R AS
(
    select
    evt_block_time as time,
    evt_block_number,
    evt_tx_hash as tx,
    evt_index,
    'Swap' as type,
    'Buy' as tx_type,
    contract_address as vault,
    nftidx as nfts,
    -1*ordinality as trade_id,
    contract_address as seller,
    "to" as buyer
    from
    Swapped_t1, unnest("nftIds") WITH ORDINALITY nftidx
    ORDER BY time
),
Txs_t as
(
    select * from Minted_t2
    UNION ALL
    select * from Redeemed_t2
    UNION ALL
    select * from Swapped_t2M
    UNION ALL
    select * from Swapped_t2R
),
Txs_t2 as
(
    select * from Txs_t
),
Txs as
(
    select * from Txs_t2 LEFT JOIN vaults on Txs_t2.vault = vaults."vaultAddress"
),
-- #################################################################
-- #################################################################


-- #################################################################
-- Aggregate Txs
-- #################################################################
agg_columns AS ( -- aggregate NFT transfers per transaction 
    SELECT
        tx,
        array_agg(nfts) AS nft_token_ids_array,
        cardinality(array_agg(nfts)) AS number_of_items,
        array_agg(seller) AS senders_array,
        array_agg(buyer) AS recipients_array,
        array_agg('erc20'::text) AS erc_types_array,
        array_agg("assetAddress") AS nft_contract_addresses_array,
        array_agg(0) AS erc_values_array
    FROM Txs
    GROUP BY 1
),
-- #################################################################
-- #################################################################

SELECT 
        'ethereum' AS blockchain,
        'NFTX' AS project,
        'v1' AS version,
        time AS block_time,
        nfts AS token_id,
        name AS collection,
        tok_price_usd AS amount_usd,
        'erc20' AS token_standard,
        'Single Item Trade' AS trade_type,
        1 AS number_of_items,
        tx_type AS trade_category,
        'Trade' AS evt_type,
        seller AS seller,
        buyer AS buyer,
        tok_price AS amount_original,
        tok_price*10^18 AS amount_raw,
        'WETH' AS currency_symbol,
        '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea AS currency_contract,
        "assetAddress" AS nft_contract_address,
        "vaultAddress" AS project_contract_address,
        NULL AS aggregator_name,
        NULL AS aggregator_address,
        evt_block_number AS block_number,
        Txs.tx AS tx_hash,
        seller AS tx_from,
        buyer AS tx_to,
        'NFTX' || '-' || Txs.tx || '-' || evt_index || '-' || trade_id as unique_trade_id
FROM Txs LEFT JOIN agg_columns on Txs.tx = agg_columns.tx
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
where time >= (select max(block_time) from {{ this }})
{% endif %}

