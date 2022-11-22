BEGIN;

DROP VIEW IF EXISTS yearn."view_ironbank_aggregate" CASCADE;
CREATE VIEW yearn."view_ironbank_aggregate" AS(
    WITH borrows AS(
        SELECT 
            payer AS address_one,
            borrower AS address_two, 
            'payer' AS address_one_type, 
            'borrower' AS address_two_type,
            "repayAmount" AS amount_one,
            "accountBorrows" AS amount_two,
            'repayAmount' AS amount_one_type, 
            'accountBorrows' AS amount_two_type, 
            NULL::bytea AS c_token_collateral,
            contract_address, 
            evt_tx_hash, 
            evt_index, 
            evt_block_time, 
            evt_block_number, 
            'RepayBorrow' AS transaction_type 
        FROM yearn."ironbank_evt_RepayBorrow"
        UNION ALL
        SELECT 
            "borrower" AS address_one,
            NULL AS address_two, 
            'borrower' AS address_one_type, 
            NULL AS address_two_type,
            "borrowAmount" AS amount_one,
            "accountBorrows" AS amount_two,
            'borrowAmount' AS amount_one_type, 
            'accountBorrows' AS amount_two_type, 
            NULL::bytea AS c_token_collateral,
            contract_address, 
            evt_tx_hash, 
            evt_index, 
            evt_block_time, 
            evt_block_number, 
            'Borrow' AS transaction_type
        FROM yearn."ironbank_evt_Borrow"
        UNION ALL
        SELECT 
            admin AS address_one,
            NULL AS address_two, 
            'admin' AS address_one_type, 
            NULL AS address_two_type,
            "reduceAmount" AS amount_one,
            "newTotalReserves" AS amount_two,
            'reduceAmount' AS amount_one_type, 
            'newTotalReserves' AS amount_two_type, 
            NULL::bytea AS c_token_collateral,
            contract_address, 
            evt_tx_hash, 
            evt_index, 
            evt_block_time, 
            evt_block_number, 
            'ReservesReduced' AS transaction_type
        FROM yearn."ironbank_evt_ReservesReduced"
    ), 

    transfers AS (
        SELECT 
            "from" AS address_one,
            "to" AS address_two, 
            'from' AS address_one_type, 
            'to' AS address_two_type,
            "amount" AS amount_one,
            NULL AS amount_two,
            'transferAmount' AS amount_one_type, 
            NULL AS amount_two_type, 
            NULL::bytea AS c_token_collateral,
            contract_address, 
            evt_tx_hash, 
            evt_index, 
            evt_block_time, 
            evt_block_number, 
            'Transfer' AS transaction_type
        FROM yearn."ironbank_evt_Transfer"
        
    ),

    two_tokens AS (
        SELECT 
            liquidator AS address_one,
            borrower AS address_two, 
            'liquidator' AS address_one_type, 
            'borrower' AS address_two_type,
            "repayAmount" AS amount_one,
            "seizeTokens" AS amount_two,
            'repayAmount' AS amount_one_type, 
            'numberTokensSeized' AS amount_two_type, 
            "cTokenCollateral" AS c_token_collateral,
            contract_address, 
            evt_tx_hash, 
            evt_index, 
            evt_block_time, 
            evt_block_number, 
            'Liquidate' AS transaction_type
        FROM yearn."ironbank_evt_LiquidateBorrow"
        
        UNION ALL
        SELECT 
            minter AS address_one,
            NULL::bytea AS address_two, 
            'minter' AS address_one_type, 
            NULL AS address_two_type,
            "mintAmount" AS amount_one,
            "mintTokens" AS amount_two,
            'mintAmount' AS amount_one_type, 
            'mintTokens' AS amount_two_type, 
            NULL::bytea AS c_token_collateral,
            contract_address, 
            evt_tx_hash, 
            evt_index, 
            evt_block_time, 
            evt_block_number, 
            'Mint' AS transaction_type
        FROM yearn."ironbank_evt_Mint"
        
        UNION ALL 
        SELECT 
            redeemer AS address_one,
            NULL AS address_two, 
            'redeemer' AS address_one_type, 
            NULL AS address_two_type,
            "redeemAmount" AS amount_one,
            "redeemTokens" AS amount_two,
            'redeemAmount' AS amount_one_type, 
            'redeemTokens' AS amount_two_type, 
            NULL::bytea AS c_token_collateral,
            contract_address, 
            evt_tx_hash, 
            evt_index, 
            evt_block_time, 
            evt_block_number, 
            'Redeem' AS transaction_type
        FROM yearn."ironbank_evt_Redeem"
        
    )


    SELECT  
        tt.address_one, 
        tt.address_two, 
        tt.address_one_type, 
        tt.address_two_type, 
        tt."c_token_collateral",
        tt.amount_one/10^yct.yvault_deposit_token_decimals AS amount_one,
        tt.amount_two/10^tok.decimals AS amount_two,
        tt.amount_one_type, 
        tt.amount_two_type, 
        tt.contract_address, 
        tt.evt_tx_hash, 
        tt.evt_index, 
        tt.evt_block_time, 
        tt.evt_block_number, 
        tt.transaction_type 
    FROM two_tokens tt
    LEFT JOIN yearn."view_yearn_contract_tokens" yct on tt.contract_address = yct.yvault_contract
    LEFT JOIN erc20."tokens" tok on yct.yvault_contract = tok.contract_address
    UNION ALL 
    SELECT  
        bo.address_one, 
        bo.address_two, 
        bo.address_one_type, 
        bo.address_two_type, 
        bo.c_token_collateral,
        bo.amount_one/10^yct.yvault_deposit_token_decimals AS amount_one,
        bo.amount_two/10^yct.yvault_deposit_token_decimals AS amount_two,
        bo.amount_one_type, bo.amount_two_type, 
        bo.contract_address, 
        bo.evt_tx_hash, 
        bo.evt_index, 
        bo.evt_block_time, 
        bo.evt_block_number, bo.transaction_type 
    FROM borrows bo
    LEFT JOIN yearn."view_yearn_contract_tokens" yct on bo.contract_address = yct.yvault_contract
    UNION ALL 
    SELECT  
        tr.address_one, 
        tr.address_two, 
        tr.address_one_type, 
        tr.address_two_type, 
        tr.c_token_collateral,
        tr.amount_one/10^tok.decimals AS amount_one,
        NULL::numeric AS amount_two,
        tr.amount_one_type, 
        tr.amount_two_type, 
        tr.contract_address, 
        tr.evt_tx_hash, 
        tr.evt_index, 
        tr.evt_block_time, 
        tr.evt_block_number, 
        tr.transaction_type 
    FROM transfers tr
    LEFT JOIN erc20."tokens" tok on tr.contract_address = tok.contract_address
 );
COMMIT;