with
  royalty_logs as (
    with
      nested_logs as (
        SELECT distinct
          call_tx_id,
          call_block_slot,
          call_outer_instruction_index,
          call_inner_instruction_index,
          cast(
            json_extract_scalar(
              json_parse(split(logs, ' ') [3]),
              '$.royalty_paid'
            ) as double
          ) as royalty,
          --   cast(
          --     json_extract_scalar(json_parse(split(logs, ' ') [3]), '$.total_price') as double
          --   ) as total_price,
          --   cast(
          --     json_extract_scalar(json_parse(split(logs, ' ') [3]), '$.lp_fee') as double
          --   ) as lp_fee,
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
              WHERE
                call_tx_id in (
                  '2CjNi6Cjiz6PnAmUsg2WXykrKxGkiZLPnpVpfnnbyjAptBJAUTiZHxVUJp9yu6JeTq8SC4ZMReieUSQwy7WX2XEq',
                  '2LWRU2w8Xk9hM1ud88hBGhEbAwd3cZpGZdd74Cthhof9jgfoHuuPM6Hj1RyoBzhjGMMUwhqXe4w4pKfBYT33STzo',
                  'So6ngQRVrjXGgBBvK3k7axoinAuP8N2LPqiJ5uFXecojLDRWuM4g7idpphXLiMCq77SWBpyQjrjSUjk9dy4hXni',
                  '4TnzVhZVxAXubo8NbMdNnZfLVyAoj7U5jBPvius7UWJU1ueksQthraDrws3ef3HJuVMyMPrA5va8QLKkLMrQA5Vk',
                  '2gHDGKbH7DRLigoz6NNduLvwRLF3dz7YnEZFgNYnWUz5m9vJ5tWexkeJtSmYwPN2AmB3xwDQ3Sx96vYyTDTFeV1v',
                  'rhG4NhUbEbcxrZqGSa5XCqGnKMTwY9GbVXiv3EZgEjYGZs9qsM5vAWi47veNgpde3H31Pv6sSa7JheAWNPPfcQG'
                )
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
              WHERE
                call_tx_id in (
                  '2CjNi6Cjiz6PnAmUsg2WXykrKxGkiZLPnpVpfnnbyjAptBJAUTiZHxVUJp9yu6JeTq8SC4ZMReieUSQwy7WX2XEq',
                  '2LWRU2w8Xk9hM1ud88hBGhEbAwd3cZpGZdd74Cthhof9jgfoHuuPM6Hj1RyoBzhjGMMUwhqXe4w4pKfBYT33STzo',
                  'So6ngQRVrjXGgBBvK3k7axoinAuP8N2LPqiJ5uFXecojLDRWuM4g7idpphXLiMCq77SWBpyQjrjSUjk9dy4hXni',
                  '4TnzVhZVxAXubo8NbMdNnZfLVyAoj7U5jBPvius7UWJU1ueksQthraDrws3ef3HJuVMyMPrA5va8QLKkLMrQA5Vk',
                  '2gHDGKbH7DRLigoz6NNduLvwRLF3dz7YnEZFgNYnWUz5m9vJ5tWexkeJtSmYwPN2AmB3xwDQ3Sx96vYyTDTFeV1v',
                  'rhG4NhUbEbcxrZqGSa5XCqGnKMTwY9GbVXiv3EZgEjYGZs9qsM5vAWi47veNgpde3H31Pv6sSa7JheAWNPPfcQG'
                )
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
              WHERE
                call_tx_id in (
                  '2CjNi6Cjiz6PnAmUsg2WXykrKxGkiZLPnpVpfnnbyjAptBJAUTiZHxVUJp9yu6JeTq8SC4ZMReieUSQwy7WX2XEq',
                  '2LWRU2w8Xk9hM1ud88hBGhEbAwd3cZpGZdd74Cthhof9jgfoHuuPM6Hj1RyoBzhjGMMUwhqXe4w4pKfBYT33STzo',
                  'So6ngQRVrjXGgBBvK3k7axoinAuP8N2LPqiJ5uFXecojLDRWuM4g7idpphXLiMCq77SWBpyQjrjSUjk9dy4hXni',
                  '4TnzVhZVxAXubo8NbMdNnZfLVyAoj7U5jBPvius7UWJU1ueksQthraDrws3ef3HJuVMyMPrA5va8QLKkLMrQA5Vk',
                  '2gHDGKbH7DRLigoz6NNduLvwRLF3dz7YnEZFgNYnWUz5m9vJ5tWexkeJtSmYwPN2AmB3xwDQ3Sx96vYyTDTFeV1v',
                  'rhG4NhUbEbcxrZqGSa5XCqGnKMTwY9GbVXiv3EZgEjYGZs9qsM5vAWi47veNgpde3H31Pv6sSa7JheAWNPPfcQG'
                )
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
              WHERE
                call_tx_id in (
                  '2CjNi6Cjiz6PnAmUsg2WXykrKxGkiZLPnpVpfnnbyjAptBJAUTiZHxVUJp9yu6JeTq8SC4ZMReieUSQwy7WX2XEq',
                  '2LWRU2w8Xk9hM1ud88hBGhEbAwd3cZpGZdd74Cthhof9jgfoHuuPM6Hj1RyoBzhjGMMUwhqXe4w4pKfBYT33STzo',
                  'So6ngQRVrjXGgBBvK3k7axoinAuP8N2LPqiJ5uFXecojLDRWuM4g7idpphXLiMCq77SWBpyQjrjSUjk9dy4hXni',
                  '4TnzVhZVxAXubo8NbMdNnZfLVyAoj7U5jBPvius7UWJU1ueksQthraDrws3ef3HJuVMyMPrA5va8QLKkLMrQA5Vk',
                  '2gHDGKbH7DRLigoz6NNduLvwRLF3dz7YnEZFgNYnWUz5m9vJ5tWexkeJtSmYwPN2AmB3xwDQ3Sx96vYyTDTFeV1v',
                  'rhG4NhUbEbcxrZqGSa5XCqGnKMTwY9GbVXiv3EZgEjYGZs9qsM5vAWi47veNgpde3H31Pv6sSa7JheAWNPPfcQG'
                )
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
              WHERE
                call_tx_id in (
                  '2CjNi6Cjiz6PnAmUsg2WXykrKxGkiZLPnpVpfnnbyjAptBJAUTiZHxVUJp9yu6JeTq8SC4ZMReieUSQwy7WX2XEq',
                  '2LWRU2w8Xk9hM1ud88hBGhEbAwd3cZpGZdd74Cthhof9jgfoHuuPM6Hj1RyoBzhjGMMUwhqXe4w4pKfBYT33STzo',
                  'So6ngQRVrjXGgBBvK3k7axoinAuP8N2LPqiJ5uFXecojLDRWuM4g7idpphXLiMCq77SWBpyQjrjSUjk9dy4hXni',
                  '4TnzVhZVxAXubo8NbMdNnZfLVyAoj7U5jBPvius7UWJU1ueksQthraDrws3ef3HJuVMyMPrA5va8QLKkLMrQA5Vk',
                  '2gHDGKbH7DRLigoz6NNduLvwRLF3dz7YnEZFgNYnWUz5m9vJ5tWexkeJtSmYwPN2AmB3xwDQ3Sx96vYyTDTFeV1v',
                  'rhG4NhUbEbcxrZqGSa5XCqGnKMTwY9GbVXiv3EZgEjYGZs9qsM5vAWi47veNgpde3H31Pv6sSa7JheAWNPPfcQG'
                )
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
              WHERE
                call_tx_id in (
                  '2CjNi6Cjiz6PnAmUsg2WXykrKxGkiZLPnpVpfnnbyjAptBJAUTiZHxVUJp9yu6JeTq8SC4ZMReieUSQwy7WX2XEq',
                  '2LWRU2w8Xk9hM1ud88hBGhEbAwd3cZpGZdd74Cthhof9jgfoHuuPM6Hj1RyoBzhjGMMUwhqXe4w4pKfBYT33STzo',
                  'So6ngQRVrjXGgBBvK3k7axoinAuP8N2LPqiJ5uFXecojLDRWuM4g7idpphXLiMCq77SWBpyQjrjSUjk9dy4hXni',
                  '4TnzVhZVxAXubo8NbMdNnZfLVyAoj7U5jBPvius7UWJU1ueksQthraDrws3ef3HJuVMyMPrA5va8QLKkLMrQA5Vk',
                  '2gHDGKbH7DRLigoz6NNduLvwRLF3dz7YnEZFgNYnWUz5m9vJ5tWexkeJtSmYwPN2AmB3xwDQ3Sx96vYyTDTFeV1v',
                  'rhG4NhUbEbcxrZqGSa5XCqGnKMTwY9GbVXiv3EZgEjYGZs9qsM5vAWi47veNgpde3H31Pv6sSa7JheAWNPPfcQG'
                )
            )
          )
          LEFT JOIN unnest (call_log_messages) as log_messages (logs) ON True
        WHERE
          logs LIKE 'Program log: {"lp_fee":%,"royalty_paid":%,"total_price":%}' --must log these fields. hopefully no other programs out there log them hahaha
          AND try(json_parse(split(logs, ' ') [3])) is not null --valid hex
      )
    SELECT
      *,
      row_number() over (
        partition by
          call_tx_id
        order by
          call_outer_instruction_index asc,
          call_inner_instruction_index asc
      ) as log_order
    FROM
      nested_logs
  ),
  priced_tokens as (
    SELECT
      symbol,
      to_base58 (contract_address) as token_mint_address
    FROM
      prices.usd_latest p
    WHERE
      p.blockchain = 'solana'
  ),
  trades as (
    SELECT
      case
        when account_buyer = call_tx_signer then 'buy'
        else 'sell'
      end as trade_category,
      case
        when contains(
          trade.call_account_arguments,
          '3dgCCb15HMQSA4Pn3Tfii5vRk7aRqTH95LJjxzsG2Mug'
        ) then 'HXD'
        when pt.token_mint_address is not null then pt.symbol
        else 'SOL'
      end as trade_token_symbol,
      case
        when contains(
          trade.call_account_arguments,
          '3dgCCb15HMQSA4Pn3Tfii5vRk7aRqTH95LJjxzsG2Mug'
        ) then '3dgCCb15HMQSA4Pn3Tfii5vRk7aRqTH95LJjxzsG2Mug'
        when pt.token_mint_address is not null then pt.token_mint_address
        else 'So11111111111111111111111111111111111111112'
      end as trade_token_mint
      --price should include all fees paid by user
,
      buyerPrice + coalesce(
        coalesce(takerFeeBp, takerFeeRaw) / 1e4 * buyerPrice,
        0
      )
      --if maker fee is negative then it is paid out of taker fee. else it comes out of taker (user) wallet
      + case
        when coalesce(
          coalesce(makerFeeBp, makerFeeRaw) / 1e4 * buyerPrice,
          0
        ) > 0 then coalesce(
          coalesce(makerFeeBp, makerFeeRaw) / 1e4 * buyerPrice,
          0
        )
        else 0
      end + coalesce(rl.royalty, 0) as price,
      makerFeeBp,
      takerFeeBp,
      makerFeeRaw,
      takerFeeRaw,
      coalesce(makerFeeBp, makerFeeRaw) / 1e4 * buyerPrice as maker_fee,
      coalesce(takerFeeBp, takerFeeRaw) / 1e4 * buyerPrice as taker_fee,
      tokenSize as token_size,
      rl.royalty --we will just be missing this if log is truncated.
,
      trade.call_instruction_name as instruction,
      trade.account_metadata,
      trade.account_tokenMint,
      trade.account_buyer,
      trade.account_seller,
      trade.call_outer_instruction_index as outer_instruction_index,
      trade.call_inner_instruction_index as inner_instruction_index,
      trade.call_block_time,
      trade.call_block_slot,
      trade.call_tx_id,
      trade.call_tx_signer
    from
      (
        (
          SELECT
            call_instruction_name,
            account_owner as account_buyer,
            call_tx_signer as account_seller,
            null as account_metadata,
            account_assetMint as account_tokenMint,
            cast(
              json_value(
                args,
                'strict $.SolFulfillBuyArgs.minPaymentAmount'
              ) as double
            ) as buyerPrice,
            cast(
              json_value(args, 'strict $.SolFulfillBuyArgs.assetAmount') as double
            ) as tokenSize,
            cast(
              json_value(args, 'strict $.SolFulfillBuyArgs.makerFeeBp') as double
            ) as makerFeeBp,
            null as makerFeeRaw,
            cast(
              json_value(args, 'strict $.SolFulfillBuyArgs.takerFeeBp') as double
            ) as takerFeeBp,
            null as takerFeeRaw,
            call_outer_instruction_index,
            call_inner_instruction_index,
            call_block_time,
            call_block_slot,
            call_tx_id,
            call_tx_signer,
            call_account_arguments,
            row_number() over (
              partition by
                call_tx_id
              order by
                call_outer_instruction_index asc,
                call_inner_instruction_index asc
            ) as call_order
          FROM
            magic_eden_solana.mmm_call_solFulfillBuy
          WHERE
            call_tx_id in (
              '2CjNi6Cjiz6PnAmUsg2WXykrKxGkiZLPnpVpfnnbyjAptBJAUTiZHxVUJp9yu6JeTq8SC4ZMReieUSQwy7WX2XEq',
              '2LWRU2w8Xk9hM1ud88hBGhEbAwd3cZpGZdd74Cthhof9jgfoHuuPM6Hj1RyoBzhjGMMUwhqXe4w4pKfBYT33STzo',
              'So6ngQRVrjXGgBBvK3k7axoinAuP8N2LPqiJ5uFXecojLDRWuM4g7idpphXLiMCq77SWBpyQjrjSUjk9dy4hXni',
              '4TnzVhZVxAXubo8NbMdNnZfLVyAoj7U5jBPvius7UWJU1ueksQthraDrws3ef3HJuVMyMPrA5va8QLKkLMrQA5Vk',
              '2gHDGKbH7DRLigoz6NNduLvwRLF3dz7YnEZFgNYnWUz5m9vJ5tWexkeJtSmYwPN2AmB3xwDQ3Sx96vYyTDTFeV1v',
              'rhG4NhUbEbcxrZqGSa5XCqGnKMTwY9GbVXiv3EZgEjYGZs9qsM5vAWi47veNgpde3H31Pv6sSa7JheAWNPPfcQG'
            )
        )
        UNION ALL
        (
          SELECT
            call_instruction_name,
            account_owner as account_buyer,
            call_tx_signer as account_seller,
            null as account_metadata,
            account_assetMint as account_tokenMint,
            cast(
              json_value(
                args,
                'strict $.SolFulfillBuyArgs.minPaymentAmount'
              ) as double
            ) as buyerPrice,
            cast(
              json_value(args, 'strict $.SolFulfillBuyArgs.assetAmount') as double
            ) as tokenSize,
            cast(
              json_value(args, 'strict $.SolFulfillBuyArgs.makerFeeBp') as double
            ) as makerFeeBp,
            null as makerFeeRaw,
            cast(
              json_value(args, 'strict $.SolFulfillBuyArgs.takerFeeBp') as double
            ) as takerFeeBp,
            null as takerFeeRaw,
            call_outer_instruction_index,
            call_inner_instruction_index,
            call_block_time,
            call_block_slot,
            call_tx_id,
            call_tx_signer,
            call_account_arguments,
            row_number() over (
              partition by
                call_tx_id
              order by
                call_outer_instruction_index asc,
                call_inner_instruction_index asc
            ) as call_order
          FROM
            magic_eden_solana.mmm_call_solMip1FulfillBuy
          WHERE
            call_tx_id in (
              '2CjNi6Cjiz6PnAmUsg2WXykrKxGkiZLPnpVpfnnbyjAptBJAUTiZHxVUJp9yu6JeTq8SC4ZMReieUSQwy7WX2XEq',
              '2LWRU2w8Xk9hM1ud88hBGhEbAwd3cZpGZdd74Cthhof9jgfoHuuPM6Hj1RyoBzhjGMMUwhqXe4w4pKfBYT33STzo',
              'So6ngQRVrjXGgBBvK3k7axoinAuP8N2LPqiJ5uFXecojLDRWuM4g7idpphXLiMCq77SWBpyQjrjSUjk9dy4hXni',
              '4TnzVhZVxAXubo8NbMdNnZfLVyAoj7U5jBPvius7UWJU1ueksQthraDrws3ef3HJuVMyMPrA5va8QLKkLMrQA5Vk',
              '2gHDGKbH7DRLigoz6NNduLvwRLF3dz7YnEZFgNYnWUz5m9vJ5tWexkeJtSmYwPN2AmB3xwDQ3Sx96vYyTDTFeV1v',
              'rhG4NhUbEbcxrZqGSa5XCqGnKMTwY9GbVXiv3EZgEjYGZs9qsM5vAWi47veNgpde3H31Pv6sSa7JheAWNPPfcQG'
            )
        )
        UNION ALL
        (
          SELECT
            call_instruction_name,
            account_owner as account_buyer,
            call_tx_signer as account_seller,
            null as account_metadata,
            account_assetMint as account_tokenMint,
            cast(
              json_value(
                args,
                'strict $.SolFulfillBuyArgs.minPaymentAmount'
              ) as double
            ) as buyerPrice,
            cast(
              json_value(args, 'strict $.SolFulfillBuyArgs.assetAmount') as double
            ) as tokenSize,
            cast(
              json_value(args, 'strict $.SolFulfillBuyArgs.makerFeeBp') as double
            ) as makerFeeBp,
            null as makerFeeRaw,
            cast(
              json_value(args, 'strict $.SolFulfillBuyArgs.takerFeeBp') as double
            ) as takerFeeBp,
            null as takerFeeRaw,
            call_outer_instruction_index,
            call_inner_instruction_index,
            call_block_time,
            call_block_slot,
            call_tx_id,
            call_tx_signer,
            call_account_arguments,
            row_number() over (
              partition by
                call_tx_id
              order by
                call_outer_instruction_index asc,
                call_inner_instruction_index asc
            ) as call_order
          FROM
            magic_eden_solana.mmm_call_solOcpFulfillBuy
          WHERE
            call_tx_id in (
              '2CjNi6Cjiz6PnAmUsg2WXykrKxGkiZLPnpVpfnnbyjAptBJAUTiZHxVUJp9yu6JeTq8SC4ZMReieUSQwy7WX2XEq',
              '2LWRU2w8Xk9hM1ud88hBGhEbAwd3cZpGZdd74Cthhof9jgfoHuuPM6Hj1RyoBzhjGMMUwhqXe4w4pKfBYT33STzo',
              'So6ngQRVrjXGgBBvK3k7axoinAuP8N2LPqiJ5uFXecojLDRWuM4g7idpphXLiMCq77SWBpyQjrjSUjk9dy4hXni',
              '4TnzVhZVxAXubo8NbMdNnZfLVyAoj7U5jBPvius7UWJU1ueksQthraDrws3ef3HJuVMyMPrA5va8QLKkLMrQA5Vk',
              '2gHDGKbH7DRLigoz6NNduLvwRLF3dz7YnEZFgNYnWUz5m9vJ5tWexkeJtSmYwPN2AmB3xwDQ3Sx96vYyTDTFeV1v',
              'rhG4NhUbEbcxrZqGSa5XCqGnKMTwY9GbVXiv3EZgEjYGZs9qsM5vAWi47veNgpde3H31Pv6sSa7JheAWNPPfcQG'
            )
        )
        UNION ALL
        (
          SELECT
            call_instruction_name,
            call_tx_signer as account_buyer,
            account_owner as account_seller,
            null as account_metadata,
            account_assetMint as account_tokenMint,
            cast(
              json_value(
                args,
                'strict $.SolFulfillSellArgs.maxPaymentAmount'
              ) as double
            ) as buyerPrice,
            cast(
              json_value(args, 'strict $.SolFulfillSellArgs.assetAmount') as double
            ) as tokenSize,
            cast(
              json_value(args, 'strict $.SolFulfillSellArgs.makerFeeBp') as double
            ) as makerFeeBp,
            null as makerFeeRaw,
            cast(
              json_value(args, 'strict $.SolFulfillSellArgs.takerFeeBp') as double
            ) as takerFeeBp,
            null as takerFeeRaw,
            call_outer_instruction_index,
            call_inner_instruction_index,
            call_block_time,
            call_block_slot,
            call_tx_id,
            call_tx_signer,
            call_account_arguments,
            row_number() over (
              partition by
                call_tx_id
              order by
                call_outer_instruction_index asc,
                call_inner_instruction_index asc
            ) as call_order
          FROM
            magic_eden_solana.mmm_call_solFulfillSell
          WHERE
            call_tx_id in (
              '2CjNi6Cjiz6PnAmUsg2WXykrKxGkiZLPnpVpfnnbyjAptBJAUTiZHxVUJp9yu6JeTq8SC4ZMReieUSQwy7WX2XEq',
              '2LWRU2w8Xk9hM1ud88hBGhEbAwd3cZpGZdd74Cthhof9jgfoHuuPM6Hj1RyoBzhjGMMUwhqXe4w4pKfBYT33STzo',
              'So6ngQRVrjXGgBBvK3k7axoinAuP8N2LPqiJ5uFXecojLDRWuM4g7idpphXLiMCq77SWBpyQjrjSUjk9dy4hXni',
              '4TnzVhZVxAXubo8NbMdNnZfLVyAoj7U5jBPvius7UWJU1ueksQthraDrws3ef3HJuVMyMPrA5va8QLKkLMrQA5Vk',
              '2gHDGKbH7DRLigoz6NNduLvwRLF3dz7YnEZFgNYnWUz5m9vJ5tWexkeJtSmYwPN2AmB3xwDQ3Sx96vYyTDTFeV1v',
              'rhG4NhUbEbcxrZqGSa5XCqGnKMTwY9GbVXiv3EZgEjYGZs9qsM5vAWi47veNgpde3H31Pv6sSa7JheAWNPPfcQG'
            )
        )
        UNION ALL
        (
          SELECT
            call_instruction_name,
            call_tx_signer as account_buyer,
            account_owner as account_seller,
            null as account_metadata,
            account_assetMint as account_tokenMint,
            cast(
              json_value(
                args,
                'strict $.SolMip1FulfillSellArgs.maxPaymentAmount'
              ) as double
            ) as buyerPrice,
            cast(
              json_value(
                args,
                'strict $.SolMip1FulfillSellArgs.assetAmount'
              ) as double
            ) as tokenSize,
            cast(
              json_value(
                args,
                'strict $.SolMip1FulfillSellArgs.makerFeeBp'
              ) as double
            ) as makerFeeBp,
            null as makerFeeRaw,
            cast(
              json_value(
                args,
                'strict $.SolMip1FulfillSellArgs.takerFeeBp'
              ) as double
            ) as takerFeeBp,
            null as takerFeeRaw,
            call_outer_instruction_index,
            call_inner_instruction_index,
            call_block_time,
            call_block_slot,
            call_tx_id,
            call_tx_signer,
            call_account_arguments,
            row_number() over (
              partition by
                call_tx_id
              order by
                call_outer_instruction_index asc,
                call_inner_instruction_index asc
            ) as call_order
          FROM
            magic_eden_solana.mmm_call_solMip1FulfillSell
          WHERE
            call_tx_id in (
              '2CjNi6Cjiz6PnAmUsg2WXykrKxGkiZLPnpVpfnnbyjAptBJAUTiZHxVUJp9yu6JeTq8SC4ZMReieUSQwy7WX2XEq',
              '2LWRU2w8Xk9hM1ud88hBGhEbAwd3cZpGZdd74Cthhof9jgfoHuuPM6Hj1RyoBzhjGMMUwhqXe4w4pKfBYT33STzo',
              'So6ngQRVrjXGgBBvK3k7axoinAuP8N2LPqiJ5uFXecojLDRWuM4g7idpphXLiMCq77SWBpyQjrjSUjk9dy4hXni',
              '4TnzVhZVxAXubo8NbMdNnZfLVyAoj7U5jBPvius7UWJU1ueksQthraDrws3ef3HJuVMyMPrA5va8QLKkLMrQA5Vk',
              '2gHDGKbH7DRLigoz6NNduLvwRLF3dz7YnEZFgNYnWUz5m9vJ5tWexkeJtSmYwPN2AmB3xwDQ3Sx96vYyTDTFeV1v',
              'rhG4NhUbEbcxrZqGSa5XCqGnKMTwY9GbVXiv3EZgEjYGZs9qsM5vAWi47veNgpde3H31Pv6sSa7JheAWNPPfcQG'
            )
        )
        UNION ALL
        (
          SELECT
            call_instruction_name,
            call_tx_signer as account_buyer,
            account_owner as account_seller,
            null as account_metadata,
            account_assetMint as account_tokenMint,
            cast(
              json_value(
                args,
                'strict $.SolOcpFulfillSellArgs.maxPaymentAmount'
              ) as double
            ) as buyerPrice,
            cast(
              json_value(
                args,
                'strict $.SolOcpFulfillSellArgs.assetAmount'
              ) as double
            ) as tokenSize,
            cast(
              json_value(args, 'strict $.SolOcpFulfillSellArgs.makerFeeBp') as double
            ) as makerFeeBp,
            null as makerFeeRaw,
            cast(
              json_value(args, 'strict $.SolOcpFulfillSellArgs.takerFeeBp') as double
            ) as takerFeeBp,
            null as takerFeeRaw,
            call_outer_instruction_index,
            call_inner_instruction_index,
            call_block_time,
            call_block_slot,
            call_tx_id,
            call_tx_signer,
            call_account_arguments,
            row_number() over (
              partition by
                call_tx_id
              order by
                call_outer_instruction_index asc,
                call_inner_instruction_index asc
            ) as call_order
          FROM
            magic_eden_solana.mmm_call_solOcpFulfillSell
          WHERE
            call_tx_id in (
              '2CjNi6Cjiz6PnAmUsg2WXykrKxGkiZLPnpVpfnnbyjAptBJAUTiZHxVUJp9yu6JeTq8SC4ZMReieUSQwy7WX2XEq',
              '2LWRU2w8Xk9hM1ud88hBGhEbAwd3cZpGZdd74Cthhof9jgfoHuuPM6Hj1RyoBzhjGMMUwhqXe4w4pKfBYT33STzo',
              'So6ngQRVrjXGgBBvK3k7axoinAuP8N2LPqiJ5uFXecojLDRWuM4g7idpphXLiMCq77SWBpyQjrjSUjk9dy4hXni',
              '4TnzVhZVxAXubo8NbMdNnZfLVyAoj7U5jBPvius7UWJU1ueksQthraDrws3ef3HJuVMyMPrA5va8QLKkLMrQA5Vk',
              '2gHDGKbH7DRLigoz6NNduLvwRLF3dz7YnEZFgNYnWUz5m9vJ5tWexkeJtSmYwPN2AmB3xwDQ3Sx96vYyTDTFeV1v',
              'rhG4NhUbEbcxrZqGSa5XCqGnKMTwY9GbVXiv3EZgEjYGZs9qsM5vAWi47veNgpde3H31Pv6sSa7JheAWNPPfcQG'
            )
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
  raw_nft_trades as (
    SELECT
      'solana' as blockchain,
      'magiceden' as project,
      'v2' as version,
      t.call_block_time as block_time,
      'secondary' as trade_type,
      token_size as number_of_items --all single trades right now
,
      t.trade_category,
      t.account_buyer as buyer,
      t.account_seller as seller,
      t.price as amount_raw --magiceden does not include fees in the emitted price
,
      t.price / pow(10, p.decimals) as amount_original,
      t.price / pow(10, p.decimals) * p.price as amount_usd,
      t.trade_token_symbol as currency_symbol,
      t.trade_token_mint as currency_address,
      cast(null as varchar) as account_merkle_tree,
      cast(null as bigint) leaf_id,
      t.account_tokenMint as account_mint,
      'M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K' as project_program_id,
      cast(null as varchar) as aggregator_name,
      cast(null as varchar) as aggregator_address,
      t.call_tx_id as tx_id,
      t.call_block_slot as block_slot,
      t.call_tx_signer as tx_signer,
      t.taker_fee as taker_fee_amount_raw --taker fees = platform fees
,
      t.taker_fee / pow(10, p.decimals) as taker_fee_amount,
      t.taker_fee / pow(10, p.decimals) * p.price as taker_fee_amount_usd,
      case
        when t.taker_fee = 0
        OR t.price = 0 then 0
        else t.taker_fee / t.price
      end as taker_fee_percentage,
      t.maker_fee as maker_fee_amount_raw,
      t.maker_fee / pow(10, p.decimals) as maker_fee_amount,
      t.maker_fee / pow(10, p.decimals) * p.price as maker_fee_amount_usd,
      case
        when t.maker_fee = 0
        OR t.price = 0 then 0
        else t.maker_fee / t.price
      end as maker_fee_percentage,
      cast(null as double) as amm_fee_amount_raw,
      cast(null as double) as amm_fee_amount,
      cast(null as double) as amm_fee_amount_usd,
      cast(null as double) as amm_fee_percentage,
      t.royalty as royalty_fee_amount_raw,
      t.royalty / pow(10, p.decimals) as royalty_fee_amount,
      t.royalty / pow(10, p.decimals) * p.price as royalty_fee_amount_usd,
      case
        when t.royalty = 0
        OR t.price = 0 then 0
        else t.royalty / t.price
      end as royalty_fee_percentage,
      t.instruction,
      t.outer_instruction_index,
      coalesce(t.inner_instruction_index, 0) as inner_instruction_index
    FROM
      trades t
      LEFT JOIN "delta_prod"."prices"."usd" p ON p.blockchain = 'solana'
      and to_base58 (p.contract_address) = t.trade_token_mint
      and p.minute = date_trunc('minute', t.call_block_time)
  )
SELECT
  *
FROM
  raw_nft_trades