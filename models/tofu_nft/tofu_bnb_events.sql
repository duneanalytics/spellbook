WITH decoded AS(
    SELECT 
        call_block_time,
        call_tx_hash,
        call_block_number,
        contract_address,
        detail.caller buyer,
        detail.currency currency,
        intent.user seller,
        from_json(detail.bundle, 'token STRING, tokenId INTEGER, amount INTEGER, kind INTEGER, mintData STRING') detail_bundle
    FROM
    (
        SELECT 
            call_block_time,
            call_tx_hash,
            call_block_number,
            contract_address,
            from_json(detail, 'intentionHash STRING, signer STRING, txDeadline INTEGER, salt STRING, id INTEGER, opcode INTEGER, caller STRING, currency STRING, price INTEGER, incentiveRate INTEGER, settlement STRING, bundle STRING, deadline INTEGER ') detail,
            from_json(intent, 'user STRING, bundle STRING, currency STRING, price INTEGER, deadline INTEGER, salt STRING, kind INTEGER') intent
        FROM tofu_nft_bnb.MarketNG_call_run)    
)
-- BNB ERC721 BUYS
    SELECT 
        'bnb' AS blockchain,
        'tofu' AS project,
        call_block_time AS block_time,
        detail_bundle.tokenId AS token_id,
        'erc721' AS token_standard,
        'Single Item Trade' AS trade_type,
        IFNULL(detail_bundle.amount, 1) AS number_of_items,
        'Sell' AS trade_category,
        decoded.seller AS seller,
        decoded.buyer AS buyer,
        CASE 
            WHEN decoded.currency ='0x0000000000000000000000000000000000000000' THEN '0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c'
            ELSE decoded.currency END AS currency_contract,
        CASE WHEN decoded.currency='0x0000000000000000000000000000000000000000' THEN 'BNB' END AS currency_symbol,
        detail_bundle.token AS nft_contract_address,
        decoded.contract_address AS project_contract_address,
        decoded.call_tx_hash AS tx_hash,
        decoded.call_block_number AS block_number
    FROM decoded
    -- {% if is_incremental() %}
    -- WHERE ee.evt_block_time >= date_trunc("day", now() - interval '1 week')
    -- {% endif %}

    UNION ALL 