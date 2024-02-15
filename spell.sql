with
  royalty_logs AS (
    with
      nested_logs AS (
        SELECT distinct
          call_tx_id,
          call_block_slot,
          call_outer_instruction_index,
          call_inner_instruction_index,
          cast(
            json_extract_scalar(
              json_parse(split(logs, ' ') [3]),
              '$.royalty_paid'
            ) AS DOUBLE
          ) AS royalty_paid,
          cast(
            json_extract_scalar(json_parse(split(logs, ' ') [3]), '$.total_price') AS DOUBLE
          ) AS total_price,
          cast(
            json_extract_scalar(json_parse(split(logs, ' ') [3]), '$.lp_fee') AS DOUBLE
          ) AS lp_fee,
          logs
        FROM
          (
            (
              SELECT
                call_tx_id,
                call_block_slot,
                call_outer_instruction_index,
                call_inner_instruction_index,
                call_log_messages
              FROM
                magic_eden_solana.mmm_call_solFulfillBuy
            )
            UNION ALL
            (
              SELECT
                call_tx_id,
                call_block_slot,
                call_outer_instruction_index,
                call_inner_instruction_index,
                call_log_messages
              FROM
                magic_eden_solana.mmm_call_solFulfillSell
            )
            UNION ALL
            (
              SELECT
                call_tx_id,
                call_block_slot,
                call_outer_instruction_index,
                call_inner_instruction_index,
                call_log_messages
              FROM
                magic_eden_solana.mmm_call_solMip1FulfillBuy
            )
            UNION ALL
            (
              SELECT
                call_tx_id,
                call_block_slot,
                call_outer_instruction_index,
                call_inner_instruction_index,
                call_log_messages
              FROM
                magic_eden_solana.mmm_call_solMip1FulfillSell
            )
            UNION ALL
            (
              SELECT
                call_tx_id,
                call_block_slot,
                call_outer_instruction_index,
                call_inner_instruction_index,
                call_log_messages
              FROM
                magic_eden_solana.mmm_call_solOcpFulfillBuy
            )
            UNION ALL
            (
              SELECT
                call_tx_id,
                call_block_slot,
                call_outer_instruction_index,
                call_inner_instruction_index,
                call_log_messages
              FROM
                magic_eden_solana.mmm_call_solOcpFulfillSell
            )
          )
          LEFT JOIN unnest (call_log_messages) AS log_messages (logs) ON True
        WHERE
          logs like 'Program log: {"lp_fee":%,"royalty_paid":%,"total_price":%}' --must log these fields. hopefully no other programs out there log them hahaha
          AND try(json_parse(split(logs, ' ') [3])) is not NULL --valid hex
      )
    SELECT
      *,
      row_number() over (
        partition BY
          call_tx_id
        ORDER BY
          call_outer_instruction_index ASC,
          call_inner_instruction_index ASC
      ) AS log_order
    FROM
      nested_logs
  ),
  priced_tokens AS (
    SELECT
      symbol,
      to_base58 (contract_address) AS token_mint_address
    FROM
      prices.usd_latest p
    WHERE
      p.blockchain = 'solana'
  ),
  trades AS (
    SELECT
      CASE
        WHEN account_buyer = call_tx_signer THEN 'buy'
        ELSE 'sell'
      END AS trade_category,
      'SOL' AS trade_token_symbol,
      'So11111111111111111111111111111111111111112' AS trade_token_mint,
      total_price AS price,
      makerFeeBp / 1e4 * rl.total_price AS maker_fee,
      takerFeeBp / 1e4 * rl.total_price AS taker_fee,
      tokenSize AS token_size,
      rl.royalty_paid AS royalty_fee,
      rl.lp_fee AS amm_fee,
      trade.call_instruction_name AS instruction,
      trade.account_tokenMint,
      trade.account_buyer,
      trade.account_seller,
      trade.call_outer_instruction_index AS outer_instruction_index,
      trade.call_inner_instruction_index AS inner_instruction_index,
      trade.call_block_time,
      trade.call_block_slot,
      trade.call_tx_id,
      trade.call_tx_signer
    from
      (
        (
          SELECT
            call_instruction_name,
            account_owner AS account_buyer,
            call_tx_signer AS account_seller,
            account_assetMint AS account_tokenMint,
            cast(
              json_value(
                args,
                'strict $.SolFulfillBuyArgs.minPaymentAmount'
              ) AS DOUBLE
            ) AS buyerPrice,
            cast(
              json_value(args, 'strict $.SolFulfillBuyArgs.assetAmount') AS DOUBLE
            ) AS tokenSize,
            cast(
              json_value(args, 'strict $.SolFulfillBuyArgs.makerFeeBp') AS DOUBLE
            ) AS makerFeeBp,
            cast(
              json_value(args, 'strict $.SolFulfillBuyArgs.takerFeeBp') AS DOUBLE
            ) AS takerFeeBp,
            call_outer_instruction_index,
            call_inner_instruction_index,
            call_block_time,
            call_block_slot,
            call_tx_id,
            call_tx_signer,
            call_account_arguments,
            row_number() over (
              partition BY
                call_tx_id
              ORDER BY
                call_outer_instruction_index ASC,
                call_inner_instruction_index ASC
            ) AS call_order
          FROM
            magic_eden_solana.mmm_call_solFulfillBuy
        )
        UNION ALL
        (
          SELECT
            call_instruction_name,
            account_owner AS account_buyer,
            call_tx_signer AS account_seller,
            account_assetMint AS account_tokenMint,
            cast(
              json_value(
                args,
                'strict $.SolFulfillBuyArgs.minPaymentAmount'
              ) AS DOUBLE
            ) AS buyerPrice,
            cast(
              json_value(args, 'strict $.SolFulfillBuyArgs.assetAmount') AS DOUBLE
            ) AS tokenSize,
            cast(
              json_value(args, 'strict $.SolFulfillBuyArgs.makerFeeBp') AS DOUBLE
            ) AS makerFeeBp,
            cast(
              json_value(args, 'strict $.SolFulfillBuyArgs.takerFeeBp') AS DOUBLE
            ) AS takerFeeBp,
            call_outer_instruction_index,
            call_inner_instruction_index,
            call_block_time,
            call_block_slot,
            call_tx_id,
            call_tx_signer,
            call_account_arguments,
            row_number() over (
              partition BY
                call_tx_id
              ORDER BY
                call_outer_instruction_index ASC,
                call_inner_instruction_index ASC
            ) AS call_order
          FROM
            magic_eden_solana.mmm_call_solMip1FulfillBuy
        )
        UNION ALL
        (
          SELECT
            call_instruction_name,
            account_owner AS account_buyer,
            call_tx_signer AS account_seller,
            account_assetMint AS account_tokenMint,
            cast(
              json_value(
                args,
                'strict $.SolFulfillBuyArgs.minPaymentAmount'
              ) AS DOUBLE
            ) AS buyerPrice,
            cast(
              json_value(args, 'strict $.SolFulfillBuyArgs.assetAmount') AS DOUBLE
            ) AS tokenSize,
            cast(
              json_value(args, 'strict $.SolFulfillBuyArgs.makerFeeBp') AS DOUBLE
            ) AS makerFeeBp,
            cast(
              json_value(args, 'strict $.SolFulfillBuyArgs.takerFeeBp') AS DOUBLE
            ) AS takerFeeBp,
            call_outer_instruction_index,
            call_inner_instruction_index,
            call_block_time,
            call_block_slot,
            call_tx_id,
            call_tx_signer,
            call_account_arguments,
            row_number() over (
              partition BY
                call_tx_id
              ORDER BY
                call_outer_instruction_index ASC,
                call_inner_instruction_index ASC
            ) AS call_order
          FROM
            magic_eden_solana.mmm_call_solOcpFulfillBuy
        )
        UNION ALL
        (
          SELECT
            call_instruction_name,
            call_tx_signer AS account_buyer,
            account_owner AS account_seller,
            account_assetMint AS account_tokenMint,
            cast(
              json_value(
                args,
                'strict $.SolFulfillSellArgs.maxPaymentAmount'
              ) AS DOUBLE
            ) AS buyerPrice,
            cast(
              json_value(args, 'strict $.SolFulfillSellArgs.assetAmount') AS DOUBLE
            ) AS tokenSize,
            cast(
              json_value(args, 'strict $.SolFulfillSellArgs.makerFeeBp') AS DOUBLE
            ) AS makerFeeBp,
            cast(
              json_value(args, 'strict $.SolFulfillSellArgs.takerFeeBp') AS DOUBLE
            ) AS takerFeeBp,
            call_outer_instruction_index,
            call_inner_instruction_index,
            call_block_time,
            call_block_slot,
            call_tx_id,
            call_tx_signer,
            call_account_arguments,
            row_number() over (
              partition BY
                call_tx_id
              ORDER BY
                call_outer_instruction_index ASC,
                call_inner_instruction_index ASC
            ) AS call_order
          FROM
            magic_eden_solana.mmm_call_solFulfillSell
        )
        UNION ALL
        (
          SELECT
            call_instruction_name,
            call_tx_signer AS account_buyer,
            account_owner AS account_seller,
            account_assetMint AS account_tokenMint,
            cast(
              json_value(
                args,
                'strict $.SolMip1FulfillSellArgs.maxPaymentAmount'
              ) AS DOUBLE
            ) AS buyerPrice,
            cast(
              json_value(
                args,
                'strict $.SolMip1FulfillSellArgs.assetAmount'
              ) AS DOUBLE
            ) AS tokenSize,
            cast(
              json_value(
                args,
                'strict $.SolMip1FulfillSellArgs.makerFeeBp'
              ) AS DOUBLE
            ) AS makerFeeBp,
            cast(
              json_value(
                args,
                'strict $.SolMip1FulfillSellArgs.takerFeeBp'
              ) AS DOUBLE
            ) AS takerFeeBp,
            call_outer_instruction_index,
            call_inner_instruction_index,
            call_block_time,
            call_block_slot,
            call_tx_id,
            call_tx_signer,
            call_account_arguments,
            row_number() over (
              partition BY
                call_tx_id
              ORDER BY
                call_outer_instruction_index ASC,
                call_inner_instruction_index ASC
            ) AS call_order
          FROM
            magic_eden_solana.mmm_call_solMip1FulfillSell
        )
        UNION ALL
        (
          SELECT
            call_instruction_name,
            call_tx_signer AS account_buyer,
            account_owner AS account_seller,
            account_assetMint AS account_tokenMint,
            cast(
              json_value(
                args,
                'strict $.SolOcpFulfillSellArgs.maxPaymentAmount'
              ) AS DOUBLE
            ) AS buyerPrice,
            cast(
              json_value(
                args,
                'strict $.SolOcpFulfillSellArgs.assetAmount'
              ) AS DOUBLE
            ) AS tokenSize,
            cast(
              json_value(args, 'strict $.SolOcpFulfillSellArgs.makerFeeBp') AS DOUBLE
            ) AS makerFeeBp,
            cast(
              json_value(args, 'strict $.SolOcpFulfillSellArgs.takerFeeBp') AS DOUBLE
            ) AS takerFeeBp,
            call_outer_instruction_index,
            call_inner_instruction_index,
            call_block_time,
            call_block_slot,
            call_tx_id,
            call_tx_signer,
            call_account_arguments,
            row_number() over (
              partition BY
                call_tx_id
              ORDER BY
                call_outer_instruction_index ASC,
                call_inner_instruction_index ASC
            ) AS call_order
          FROM
            magic_eden_solana.mmm_call_solOcpFulfillSell
        )
      ) trade
      LEFT JOIN royalty_logs rl ON trade.call_tx_id = rl.call_tx_id
      AND trade.call_block_slot = rl.call_block_slot
      AND trade.call_order = rl.log_order
      LEFT JOIN priced_tokens pt ON contains(
        trade.call_account_arguments,
        pt.token_mint_address
      )
  ),
  raw_nft_trades AS (
    SELECT
      'solana' AS blockchain,
      'magiceden' AS project,
      'mmm' AS version,
      t.call_block_time AS block_time,
      'secondary' AS trade_type,
      token_size AS number_of_items,
      t.trade_category,
      t.account_buyer AS buyer,
      t.account_seller AS seller,
      t.price AS amount_raw,
      t.price / pow(10, p.decimals) AS amount_original,
      t.price / pow(10, p.decimals) * p.price AS amount_usd,
      t.trade_token_symbol AS currency_symbol,
      t.trade_token_mint AS currency_address,
      cast(NULL AS VARCHAR) AS account_merkle_tree,
      cast(NULL AS BIGINT) leaf_id,
      t.account_tokenMint AS account_mint,
      'mmm3XBJg5gk8XJxEKBvdgptZz6SgK4tXvn36sodowMc' AS project_program_id,
      cast(NULL AS VARCHAR) AS aggregator_name,
      cast(NULL AS VARCHAR) AS aggregator_address,
      t.call_tx_id AS tx_id,
      t.call_block_slot AS block_slot,
      t.call_tx_signer AS tx_signer,
      t.taker_fee AS taker_fee_amount_raw,
      t.taker_fee / pow(10, p.decimals) AS taker_fee_amount,
      t.taker_fee / pow(10, p.decimals) * p.price AS taker_fee_amount_usd,
      CASE
        WHEN t.taker_fee = 0
        OR t.price = 0 THEN 0
        ELSE t.taker_fee / t.price
      END AS taker_fee_percentage,
      t.maker_fee AS maker_fee_amount_raw,
      t.maker_fee / pow(10, p.decimals) AS maker_fee_amount,
      t.maker_fee / pow(10, p.decimals) * p.price AS maker_fee_amount_usd,
      CASE
        WHEN t.maker_fee = 0
        OR t.price = 0 THEN 0
        ELSE t.maker_fee / t.price
      END AS maker_fee_percentage,
      t.amm_fee AS amm_fee_amount_raw,
      t.amm_fee / pow(10, p.decimals) AS amm_fee_amount,
      t.amm_fee / pow(10, p.decimals) * p.price AS amm_fee_amount_usd,
      CASE
        WHEN t.amm_fee = 0
        OR t.price = 0 THEN 0
        ELSE t.amm_fee / t.price
      END AS amm_fee_percentage,
      t.royalty_fee AS royalty_fee_amount_raw,
      t.royalty_fee / pow(10, p.decimals) AS royalty_fee_amount,
      t.royalty_fee / pow(10, p.decimals) * p.price AS royalty_fee_amount_usd,
      CASE
        WHEN t.royalty_fee = 0
        OR t.price = 0 THEN 0
        ELSE t.royalty_fee / t.price
      END AS royalty_fee_percentage,
      t.instruction,
      t.outer_instruction_index,
      COALESCE(t.inner_instruction_index, 0) AS inner_instruction_index
    FROM
      trades t
      LEFT JOIN "delta_prod"."prices"."usd" p ON p.blockchain = 'solana'
      AND to_base58 (p.contract_address) = t.trade_token_mint
      AND p.minute = date_trunc('minute', t.call_block_time)
  )
SELECT
  *
FROM
  raw_nft_trades