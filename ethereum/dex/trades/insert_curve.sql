CREATE OR REPLACE FUNCTION dex.insert_curve(start_ts timestamptz, end_ts timestamptz=now(), start_block numeric=0, end_block numeric=9e18) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN
WITH rows AS (
    INSERT INTO dex.trades (
        block_time,
        token_a_symbol,
        token_b_symbol,
        token_a_amount,
        token_b_amount,
        project,
        version,
        category,
        trader_a,
        trader_b,
        token_a_amount_raw,
        token_b_amount_raw,
        usd_amount,
        token_a_address,
        token_b_address,
        exchange_contract_address,
        tx_hash,
        tx_from,
        tx_to,
        trace_address,
        evt_index,
        trade_id
    )

    WITH exchange_n_exchangUnderlying AS (

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."17PctCypt_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."2CRV_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."3DYDX_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."3eurpool_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."aave_swap_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."aETHb_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."AETHV1_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."ag_ibEUR_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."agEURsEUR_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."alcxeth_swap_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."aleth_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."alusd_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."aMATICb_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."ankreth_swap_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."aUSDC_aDAI_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."badgerwbtc_swap_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."baoUSD_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."bbtc_swap_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."bean_lusd_pool_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."bentcvx_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."bhome_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."BTCpx_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."btrfly_eth_swap_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."busd_swap_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."busdv2_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."bveCVX_CVX_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."cadcusdc_swap_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."compound_swap_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."crvCRV_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."crvCRVsCRV_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."crveth_swap_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."cvxcrv_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."cvxeth_swap_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."cvxfxsfxs_swap_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."D3_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."dei_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."dola_3crv_pool_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."DSU_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."DSU3Crv_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."ducketh_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."dusd_swap_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."dydxeth_swap_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."ELONXSWAP_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."ETH_vETH2_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."EURN_EURT_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."eurs_swap_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."eursusdc_swap_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."eurt_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."eurt_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."eurtusd_swap_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM fei_protocol."Fei3Crv_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."FEIPCV_1_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."fiat3crv_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."frax_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."fUSD_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."fxEUR_CRV_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."fxseth_swap_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."fxseth_swap_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."fxseth_swap_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."fxseth_swap_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."gusd_swap_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."hbtc_swap_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."husd_swap_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."ibAUD_sAUD_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."ibbtc_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."ibbtc_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."ibCHF_sCHF_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."ibEUR_sEUR_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."ibEUR_sEUR_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."ibGBP_sGBP_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."ibJPY_sJPY_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."ibKRW_sKRW_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."ibZAR_ZARP_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."invdola_swap_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."ironbank_swap_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."jGBP_TGBP_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."JPYC_ibJPY_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."kusd3pool_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."link_swap_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."linkusd_swap_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."lusd_swap_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."mEUR_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."mim_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."MIM_UST_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."musd_swap_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."obtc_swap_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."OPEN_MATIC_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."ORK_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."palstkaave_swap_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."par3crv_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."parusdc_swap_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."pax_swap_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."paxusdp_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."pbtc_swap_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."PWRD_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."PWRD3CRV_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."QBITWELLS_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."QWell1_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."rai_swap_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."raiageur_swap_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."raieth_swap_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."raifrax_swap_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."RAMPrUSD_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."ren_swap_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."reth_swap_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."rETHwstETH_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."rpeth_swap_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."rsv_swap_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."saave_swap_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."sansUSDT_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."sbtc_swap_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."sdCRV_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."sdteth_swap_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."seth_swap_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."spelleth_swap_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."steth_swap_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."STG_USDC_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."sUSD_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."susd_swap_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."tALCX_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."tAPW_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."tbtc_swap_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."tbtc2_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."teth_swap_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."tFOX_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."tFRAXFRAX_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."tFXS_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."tGAMMA_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."threepool_swap_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."TOKEETH_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."tpd_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."tricrypto_swap_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."tricrypto2_swap_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."tSNX_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."tSUSHI_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."tTCR_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."tusd_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."tusd3pool_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."tWETH_WETH_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."usdd_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."usdk_swap_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."usdm_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."usdn_swap_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."usdp_swap_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."usdt_swap_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."usdv_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."ust_swap_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."USTFRAX_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."USX3CRV_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."waBTC_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."wormhole_v2_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."xautusd_swap_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."XIM_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."XIM3CRV_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."XSTUSD_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."y_swap_evt_TokenExchange"
            UNION ALL

        SELECT evt_block_time, buyer, tokens_bought, tokens_sold, bought_id, sold_id, contract_address, evt_tx_hash, evt_index
            FROM curvefi."yfieth_swap_evt_TokenExchange"
            UNION ALL

      SELECT call_block_time, buyer, output_0, "dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."tusd_call_exchange_underlying" c
            INNER JOIN curvefi."tusd_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL
        SELECT call_block_time, buyer, output_0, "dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."tusd_call_exchange_underlying0" c
            INNER JOIN curvefi."tusd_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL

        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."rai_swap_call_exchange_underlying" c
            INNER JOIN curvefi."rai_swap_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL

        SELECT call_block_time, buyer, output_0, "dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."gusd_swap_call_exchange_underlying" c
            INNER JOIN curvefi."gusd_swap_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL

        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."USX3CRV_call_exchange_underlying" c
            INNER JOIN curvefi."USX3CRV_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL
        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."USX3CRV_call_exchange_underlying0" c
            INNER JOIN curvefi."USX3CRV_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL

        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."usdd_call_exchange_underlying" c
            INNER JOIN curvefi."usdd_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL
        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."usdd_call_exchange_underlying0" c
            INNER JOIN curvefi."usdd_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL

        SELECT call_block_time, buyer, output_0, "dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."linkusd_swap_call_exchange_underlying" c
            INNER JOIN curvefi."linkusd_swap_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL

        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."usdp_swap_call_exchange_underlying" c
            INNER JOIN curvefi."usdp_swap_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL

        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."tbtc2_call_exchange_underlying" c
            INNER JOIN curvefi."tbtc2_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL
        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."tbtc2_call_exchange_underlying0" c
            INNER JOIN curvefi."tbtc2_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL

        SELECT call_block_time, buyer, output_0, "dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."rsv_swap_call_exchange_underlying" c
            INNER JOIN curvefi."rsv_swap_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL

        SELECT call_block_time, buyer, output_0, "dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."usdk_swap_call_exchange_underlying" c
            INNER JOIN curvefi."usdk_swap_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL

        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."usdm_call_exchange_underlying" c
            INNER JOIN curvefi."usdm_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL
        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."usdm_call_exchange_underlying0" c
            INNER JOIN curvefi."usdm_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL

        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."kusd3pool_call_exchange_underlying" c
            INNER JOIN curvefi."kusd3pool_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL
        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."kusd3pool_call_exchange_underlying0" c
            INNER JOIN curvefi."kusd3pool_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL

        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."AETHV1_call_exchange_underlying" c
            INNER JOIN curvefi."AETHV1_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL
        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."AETHV1_call_exchange_underlying0" c
            INNER JOIN curvefi."AETHV1_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL

        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."tpd_call_exchange_underlying" c
            INNER JOIN curvefi."tpd_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL
        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."tpd_call_exchange_underlying0" c
            INNER JOIN curvefi."tpd_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL

        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."fUSD_call_exchange_underlying" c
            INNER JOIN curvefi."fUSD_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL
        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."fUSD_call_exchange_underlying0" c
            INNER JOIN curvefi."fUSD_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL

        SELECT call_block_time, buyer, output_0, "dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."busdv2_call_exchange_underlying" c
            INNER JOIN curvefi."busdv2_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL

        SELECT call_block_time, buyer, output_0, "dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."pbtc_swap_call_exchange_underlying" c
            INNER JOIN curvefi."pbtc_swap_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL

        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."XIM3CRV_call_exchange_underlying" c
            INNER JOIN curvefi."XIM3CRV_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL
        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."XIM3CRV_call_exchange_underlying0" c
            INNER JOIN curvefi."XIM3CRV_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL

        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."XIM_call_exchange_underlying" c
            INNER JOIN curvefi."XIM_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL
        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."XIM_call_exchange_underlying0" c
            INNER JOIN curvefi."XIM_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL

        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."sUSD_call_exchange_underlying" c
            INNER JOIN curvefi."sUSD_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL
        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."sUSD_call_exchange_underlying0" c
            INNER JOIN curvefi."sUSD_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL

        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."fiat3crv_call_exchange_underlying" c
            INNER JOIN curvefi."fiat3crv_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL
        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."fiat3crv_call_exchange_underlying0" c
            INNER JOIN curvefi."fiat3crv_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL

        SELECT call_block_time, buyer, output_0, "dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."dusd_swap_call_exchange_underlying" c
            INNER JOIN curvefi."dusd_swap_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL

        SELECT call_block_time, buyer, output_0, "dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."lusd_swap_call_exchange_underlying" c
            INNER JOIN curvefi."lusd_swap_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL

        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."DSU3Crv_call_exchange_underlying" c
            INNER JOIN curvefi."DSU3Crv_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL
        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."DSU3Crv_call_exchange_underlying0" c
            INNER JOIN curvefi."DSU3Crv_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL

        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."DSU_call_exchange_underlying" c
            INNER JOIN curvefi."DSU_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL
        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."DSU_call_exchange_underlying0" c
            INNER JOIN curvefi."DSU_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL

        SELECT call_block_time, buyer, output_0, "dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."usdn_swap_call_exchange_underlying" c
            INNER JOIN curvefi."usdn_swap_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL

        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."par3crv_call_exchange_underlying" c
            INNER JOIN curvefi."par3crv_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL
        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."par3crv_call_exchange_underlying0" c
            INNER JOIN curvefi."par3crv_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL

        SELECT call_block_time, buyer, tokens_bought, "dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."y_swap_call_exchange_underlying" c
            INNER JOIN curvefi."y_swap_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL

        SELECT call_block_time, buyer, tokens_bought, "dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."usdt_swap_call_exchange_underlying" c
            INNER JOIN curvefi."usdt_swap_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL

        SELECT call_block_time, buyer, tokens_bought, "dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."susd_swap_call_exchange_underlying" c
            INNER JOIN curvefi."susd_swap_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL

        SELECT call_block_time, buyer, output_0, "dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."saave_swap_call_exchange_underlying" c
            INNER JOIN curvefi."saave_swap_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL

        SELECT call_block_time, buyer, tokens_bought, "dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."pax_swap_call_exchange_underlying" c
            INNER JOIN curvefi."pax_swap_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL

        SELECT call_block_time, buyer, output_0, "dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."ironbank_swap_call_exchange_underlying" c
            INNER JOIN curvefi."ironbank_swap_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL

        SELECT call_block_time, buyer, tokens_bought, "dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."compound_swap_call_exchange_underlying" c
            INNER JOIN curvefi."compound_swap_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL

        SELECT call_block_time, buyer, tokens_bought, "dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."busd_swap_call_exchange_underlying" c
            INNER JOIN curvefi."busd_swap_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL

        SELECT call_block_time, buyer, output_0, "dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."aave_swap_call_exchange_underlying" c
            INNER JOIN curvefi."aave_swap_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL

        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."baoUSD_call_exchange_underlying" c
            INNER JOIN curvefi."baoUSD_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL
        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."baoUSD_call_exchange_underlying0" c
            INNER JOIN curvefi."baoUSD_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL

        SELECT call_block_time, buyer, output_0, "dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."obtc_swap_call_exchange_underlying" c
            INNER JOIN curvefi."obtc_swap_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL

        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."ELONXSWAP_call_exchange_underlying" c
            INNER JOIN curvefi."ELONXSWAP_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL
        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."ELONXSWAP_call_exchange_underlying0" c
            INNER JOIN curvefi."ELONXSWAP_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL

        SELECT call_block_time, buyer, output_0, "dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."frax_call_exchange_underlying" c
            INNER JOIN curvefi."frax_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL
        SELECT call_block_time, buyer, output_0, "dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."frax_call_exchange_underlying0" c
            INNER JOIN curvefi."frax_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL

        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."dola_3crv_pool_call_exchange_underlying" c
            INNER JOIN curvefi."dola_3crv_pool_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL
        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."dola_3crv_pool_call_exchange_underlying0" c
            INNER JOIN curvefi."dola_3crv_pool_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL

        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."ibbtc_call_exchange_underlying" c
            INNER JOIN curvefi."ibbtc_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL
        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."ibbtc_call_exchange_underlying0" c
            INNER JOIN curvefi."ibbtc_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL

        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."QWell1_call_exchange_underlying" c
            INNER JOIN curvefi."QWell1_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL
        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."QWell1_call_exchange_underlying0" c
            INNER JOIN curvefi."QWell1_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL

        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."QBITWELLS_call_exchange_underlying" c
            INNER JOIN curvefi."QBITWELLS_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL
        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."QBITWELLS_call_exchange_underlying0" c
            INNER JOIN curvefi."QBITWELLS_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL

        SELECT call_block_time, buyer, output_0, "dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."tbtc_swap_call_exchange_underlying" c
            INNER JOIN curvefi."tbtc_swap_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL

        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."paxusdp_call_exchange_underlying" c
            INNER JOIN curvefi."paxusdp_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL
        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."paxusdp_call_exchange_underlying0" c
            INNER JOIN curvefi."paxusdp_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL

        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."3DYDX_call_exchange_underlying" c
            INNER JOIN curvefi."3DYDX_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL
        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."3DYDX_call_exchange_underlying0" c
            INNER JOIN curvefi."3DYDX_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL

        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM fei_protocol."Fei3Crv_call_exchange_underlying" c
            INNER JOIN fei_protocol."Fei3Crv_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL
        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM fei_protocol."Fei3Crv_call_exchange_underlying0" c
            INNER JOIN fei_protocol."Fei3Crv_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL

        SELECT call_block_time, buyer, output_0, "dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."mim_call_exchange_underlying" c
            INNER JOIN curvefi."mim_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL
        SELECT call_block_time, buyer, output_0, "dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."mim_call_exchange_underlying0" c
            INNER JOIN curvefi."mim_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL

        SELECT call_block_time, buyer, output_0, "dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."bbtc_swap_call_exchange_underlying" c
            INNER JOIN curvefi."bbtc_swap_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL

        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."BTCpx_call_exchange_underlying" c
            INNER JOIN curvefi."BTCpx_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL
        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."BTCpx_call_exchange_underlying0" c
            INNER JOIN curvefi."BTCpx_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL

        SELECT call_block_time, buyer, output_0, "dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."ust_swap_call_exchange_underlying" c
            INNER JOIN curvefi."ust_swap_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL

        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."wormhole_v2_call_exchange_underlying" c
            INNER JOIN curvefi."wormhole_v2_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL
        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."wormhole_v2_call_exchange_underlying0" c
            INNER JOIN curvefi."wormhole_v2_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL

        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."bhome_call_exchange_underlying" c
            INNER JOIN curvefi."bhome_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL
        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."bhome_call_exchange_underlying0" c
            INNER JOIN curvefi."bhome_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL

        SELECT call_block_time, buyer, output_0, "dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."alusd_call_exchange_underlying" c
            INNER JOIN curvefi."alusd_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL
        SELECT call_block_time, buyer, output_0, "dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."alusd_call_exchange_underlying0" c
            INNER JOIN curvefi."alusd_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL

        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."17PctCypt_call_exchange_underlying" c
            INNER JOIN curvefi."17PctCypt_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL
        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."17PctCypt_call_exchange_underlying0" c
            INNER JOIN curvefi."17PctCypt_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL

        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."17PctCypt_call_exchange_underlying" c
            INNER JOIN curvefi."17PctCypt_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL
        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."17PctCypt_call_exchange_underlying0" c
            INNER JOIN curvefi."17PctCypt_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL

        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."ibbtc_call_exchange_underlying" c
            INNER JOIN curvefi."ibbtc_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL
        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."ibbtc_call_exchange_underlying0" c
            INNER JOIN curvefi."ibbtc_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL

        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."XSTUSD_call_exchange_underlying" c
            INNER JOIN curvefi."XSTUSD_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL
        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."XSTUSD_call_exchange_underlying0" c
            INNER JOIN curvefi."XSTUSD_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL

        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."RAMPrUSD_call_exchange_underlying" c
            INNER JOIN curvefi."RAMPrUSD_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL
        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."RAMPrUSD_call_exchange_underlying0" c
            INNER JOIN curvefi."RAMPrUSD_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL

        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."dei_call_exchange_underlying" c
            INNER JOIN curvefi."dei_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL
        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."dei_call_exchange_underlying0" c
            INNER JOIN curvefi."dei_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL

        SELECT call_block_time, buyer, output_0, "dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."husd_swap_call_exchange_underlying" c
            INNER JOIN curvefi."husd_swap_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL

        SELECT call_block_time, buyer, output_0, "dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."ORK_call_exchange_underlying" c
            INNER JOIN curvefi."ORK_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL
        SELECT call_block_time, buyer, output_0, "dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."ORK_call_exchange_underlying0" c
            INNER JOIN curvefi."ORK_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL

        SELECT call_block_time, buyer, output_0, "dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."ORK_call_exchange_underlying" c
            INNER JOIN curvefi."ORK_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL
        SELECT call_block_time, buyer, output_0, "dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."ORK_call_exchange_underlying0" c
            INNER JOIN curvefi."ORK_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL

        SELECT call_block_time, buyer, output_0, "dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."musd_swap_call_exchange_underlying" c
            INNER JOIN curvefi."musd_swap_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL

        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."usdv_call_exchange_underlying" c
            INNER JOIN curvefi."usdv_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL
        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."usdv_call_exchange_underlying0" c
            INNER JOIN curvefi."usdv_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL

        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."tusd3pool_call_exchange_underlying" c
            INNER JOIN curvefi."tusd3pool_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL
        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."tusd3pool_call_exchange_underlying0" c
            INNER JOIN curvefi."tusd3pool_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL

        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."PWRD3CRV_call_exchange_underlying" c
            INNER JOIN curvefi."PWRD3CRV_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL
        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."PWRD3CRV_call_exchange_underlying0" c
            INNER JOIN curvefi."PWRD3CRV_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL

        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."PWRD_call_exchange_underlying" c
            INNER JOIN curvefi."PWRD_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL
        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."PWRD_call_exchange_underlying0" c
            INNER JOIN curvefi."PWRD_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL

        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."waBTC_call_exchange_underlying" c
            INNER JOIN curvefi."waBTC_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
        UNION ALL
        SELECT call_block_time, buyer, output_0, "_dx", j, i, c.contract_address, call_tx_hash, evt_index
            FROM curvefi."waBTC_call_exchange_underlying0" c
            INNER JOIN curvefi."waBTC_evt_TokenExchangeUnderlying" e ON evt_tx_hash = call_tx_hash AND i = sold_id AND j = bought_id
            WHERE call_success
    ),
    
    pools AS (
        SELECT   
            version,
            name,
            symbol,
            pool_address,
            CASE WHEN coin0 = '\x0000000000000000000000000000000000000000' THEN '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' ELSE coin0 END AS coin0,
            CASE WHEN coin1 = '\x0000000000000000000000000000000000000000' THEN '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' ELSE coin1 END AS coin1,
            CASE WHEN coin2 = '\x0000000000000000000000000000000000000000' THEN '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' ELSE coin2 END AS coin2,
            CASE WHEN coin3 = '\x0000000000000000000000000000000000000000' THEN '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' ELSE coin3 END AS coin3,
            undercoin0,
            undercoin1,
            undercoin2,
            undercoin3
        FROM curvefi."view_pools"
    ),

    curve_trades AS (
        SELECT
            evt_block_time AS block_time,
            'Curve' AS project,
            vp.version AS version,
            buyer AS trader_a,
            NULL::bytea AS trader_b,
            tokens_bought AS token_a_amount_raw,
            tokens_sold AS token_b_amount_raw,
            CASE
                WHEN bought_id = 0 THEN coin0
                WHEN bought_id = 1 THEN coin1
                WHEN bought_id = 2 THEN coin2
                WHEN bought_id = 3 THEN coin3
            END AS token_a_address,
            CASE
                WHEN sold_id = 0 THEN coin0
                WHEN sold_id = 1 THEN coin1
                WHEN sold_id = 2 THEN coin2
                WHEN sold_id = 3 THEN coin3
            END AS token_b_address,
            contract_address AS exchange_contract_address,
            evt_tx_hash AS tx_hash,
            NULL::integer[] AS trace_address,
            evt_index
        FROM exchange_n_exchangUnderlying e
        LEFT JOIN curvefi."view_pools" vp ON e.contract_address = vp.pool_address
    )

    SELECT
        ct.block_time,
        erc20a.symbol AS token_a_symbol,
        erc20b.symbol AS token_b_symbol,
        token_a_amount_raw / 10 ^ erc20a.decimals AS token_a_amount,
        token_b_amount_raw / 10 ^ erc20b.decimals AS token_b_amount,
        project,
        version,
        'DEX' AS category,
        coalesce(trader_a, tx."from") as trader_a, -- subqueries rely on this COALESCE to avoid redundant joins with the transactions table
        trader_b,
        token_a_amount_raw,
        token_b_amount_raw,
        coalesce(
            token_a_amount_raw / 10 ^ pa.decimals * pa.price,
            token_b_amount_raw / 10 ^ pb.decimals * pb.price
        ) as usd_amount,
        token_a_address,
        token_b_address,
        exchange_contract_address,
        tx_hash,
        tx."from" as tx_from,
        tx."to" as tx_to,
        trace_address,
        evt_index,
        row_number() OVER (PARTITION BY project, tx_hash, evt_index, trace_address ORDER BY version) AS trade_id
    FROM curve_trades ct
    INNER JOIN ethereum.transactions tx ON ct.tx_hash = tx.hash
            AND tx.block_time >= start_ts
            AND tx.block_time < end_ts
            AND tx.block_number >= start_block
            AND tx.block_number < end_block
    LEFT JOIN erc20.tokens erc20a ON erc20a.contract_address = ct.token_a_address
    LEFT JOIN erc20.tokens erc20b ON erc20b.contract_address = ct.token_b_address
    LEFT JOIN prices.usd pa ON pa.minute = date_trunc('minute', ct.block_time)
        AND pa.contract_address = ct.token_a_address
        AND pa.minute >= start_ts
        AND pa.minute < end_ts
    LEFT JOIN prices.usd pb ON pb.minute = date_trunc('minute', ct.block_time)
        AND pb.contract_address = ct.token_b_address
        AND pb.minute >= start_ts
        AND pb.minute < end_ts

    ON CONFLICT DO NOTHING
    RETURNING 1
)
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;

-- fill 2020
SELECT dex.insert_curve(
    '2020-01-01',
    '2021-01-01',
    (SELECT max(number) FROM ethereum.blocks WHERE time < '2020-01-01'),
    (SELECT max(number) FROM ethereum.blocks WHERE time <= '2021-01-01')
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.trades
    WHERE block_time > '2020-01-01'
    AND block_time <= '2021-01-01'
    AND project = 'Curve'
);

-- fill 2021
SELECT dex.insert_curve(
    '2021-01-01',
    '2022-01-01',
    (SELECT max(number) FROM ethereum.blocks WHERE time < '2021-01-01'),
    (SELECT max(number) FROM ethereum.blocks WHERE time <= '2022-01-01')
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.trades
    WHERE block_time > '2021-01-01'
    AND block_time <= '2022-01-01'
    AND project = 'Curve'
);

-- fill 2022
SELECT dex.insert_curve(
    '2022-01-01',
    now(),
    (SELECT max(number) FROM ethereum.blocks WHERE time < '2022-01-01'),
    (SELECT MAX(number) FROM ethereum.blocks where time < now() - interval '20 minutes')
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.trades
    WHERE block_time > '2022-01-01'
    AND block_time <= now() - interval '20 minutes'
    AND project = 'Curve'
);

INSERT INTO cron.job (schedule, command)
VALUES ('*/10 * * * *', $$
    SELECT dex.insert_curve(
        (SELECT max(block_time) - interval '1 days' FROM dex.trades WHERE project='Curve'),
        (SELECT now() - interval '20 minutes'),
        (SELECT max(number) FROM ethereum.blocks WHERE time < (SELECT max(block_time) - interval '1 days' FROM dex.trades WHERE project='Curve')),
        (SELECT MAX(number) FROM ethereum.blocks where time < now() - interval '20 minutes'));
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
