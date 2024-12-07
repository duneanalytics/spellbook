WITH
  -- ethereum
  socket_v2_ethereum AS (
    SELECT
      contract_address,
      evt_tx_hash,
      evt_index,
      evt_block_time,
      evt_block_number,
      amount,
      token,
      toChainId,
      bridgeName,
      sender,
      receiver,
      metadata
    FROM
      socket_v2_ethereum.SocketGateway_evt_SocketBridge
    WHERE
      evt_block_time >= CURRENT_TIMESTAMP - INTERVAL '1' DAY
  ),
  -- zkevm
  socket_v2_zkevm AS (
    SELECT
      contract_address,
      evt_tx_hash,
      evt_index,
      evt_block_time,
      evt_block_number,
      amount,
      token,
      toChainId,
      bridgeName,
      sender,
      receiver,
      metadata
    FROM
      socket_v2_zkevm.SocketGateway_evt_SocketBridge
    WHERE
      evt_block_time >= CURRENT_TIMESTAMP - INTERVAL '1' DAY
  ),
  -- scroll
  socket_v2_scroll AS (
    SELECT
      contract_address,
      evt_tx_hash,
      evt_index,
      evt_block_time,
      evt_block_number,
      amount,
      token,
      toChainId,
      bridgeName,
      sender,
      receiver,
      metadata
    FROM
      socket_v2_scroll.SocketGateway_evt_SocketBridge
    WHERE
      evt_block_time >= CURRENT_TIMESTAMP - INTERVAL '1' DAY
  ),
  -- blast
  socket_v2_blast AS (
    SELECT
      contract_address,
      evt_tx_hash,
      evt_index,
      evt_block_time,
      evt_block_number,
      amount,
      token,
      toChainId,
      bridgeName,
      sender,
      receiver,
      metadata
    FROM
      socket_v2_blast.SocketGateway_evt_SocketBridge
    WHERE
      evt_block_time >= CURRENT_TIMESTAMP - INTERVAL '1' DAY
  ),
  -- linea
  socket_v2_linea AS (
    SELECT
      contract_address,
      evt_tx_hash,
      evt_index,
      evt_block_time,
      evt_block_number,
      amount,
      token,
      toChainId,
      bridgeName,
      sender,
      receiver,
      metadata
    FROM
      socket_v2_linea.SocketGateway_evt_SocketBridge
    WHERE
      evt_block_time >= CURRENT_TIMESTAMP - INTERVAL '1' DAY
  ),
  -- mantle
  socket_v2_mantle AS (
    SELECT
      contract_address,
      evt_tx_hash,
      evt_index,
      evt_block_time,
      evt_block_number,
      amount,
      token,
      toChainId,
      bridgeName,
      sender,
      receiver,
      metadata
    FROM
      socket_v2_mantle.SocketGateway_evt_SocketBridge
    WHERE
      evt_block_time >= CURRENT_TIMESTAMP - INTERVAL '1' DAY
  ),
  -- optimism
  socket_v2_optimism AS (
    SELECT
      contract_address,
      evt_tx_hash,
      evt_index,
      evt_block_time,
      evt_block_number,
      amount,
      token,
      toChainId,
      bridgeName,
      sender,
      receiver,
      metadata
    FROM
      socket_v2_optimism.SocketGateway_evt_SocketBridge
    WHERE
      evt_block_time >= CURRENT_TIMESTAMP - INTERVAL '1' DAY
  ),
  -- gnosis
  socket_v2_gnosis AS (
    SELECT
      contract_address,
      evt_tx_hash,
      evt_index,
      evt_block_time,
      evt_block_number,
      amount,
      token,
      toChainId,
      bridgeName,
      sender,
      receiver,
      metadata
    FROM
      socket_v2_gnosis.SocketGateway_evt_SocketBridge
    WHERE
      evt_block_time >= CURRENT_TIMESTAMP - INTERVAL '1' DAY
  ),
  -- arbitrum
  socket_v2_arbitrum AS (
    SELECT
      contract_address,
      evt_tx_hash,
      evt_index,
      evt_block_time,
      evt_block_number,
      amount,
      token,
      toChainId,
      bridgeName,
      sender,
      receiver,
      metadata
    FROM
      socket_v2_arbitrum.SocketGateway_evt_SocketBridge
    WHERE
      evt_block_time >= CURRENT_TIMESTAMP - INTERVAL '1' DAY
  ),
  -- zksync
  socket_v2_zksync AS (
    SELECT
      contract_address,
      evt_tx_hash,
      evt_index,
      evt_block_time,
      evt_block_number,
      amount,
      token,
      toChainId,
      bridgeName,
      sender,
      receiver,
      metadata
    FROM
      socket_v2_zksync.SocketGateway_evt_SocketBridge
    WHERE
      evt_block_time >= CURRENT_TIMESTAMP - INTERVAL '1' DAY
  ),
  -- base
  socket_v2_base AS (
    SELECT
      contract_address,
      evt_tx_hash,
      evt_index,
      evt_block_time,
      evt_block_number,
      amount,
      token,
      toChainId,
      bridgeName,
      sender,
      receiver,
      metadata
    FROM
      socket_v2_base.SocketGateway_evt_SocketBridge
    WHERE
      evt_block_time >= CURRENT_TIMESTAMP - INTERVAL '1' DAY
  ),
  -- bnb
  socket_v2_bnb AS (
    SELECT
      contract_address,
      evt_tx_hash,
      evt_index,
      evt_block_time,
      evt_block_number,
      amount,
      token,
      toChainId,
      bridgeName,
      sender,
      receiver,
      metadata
    FROM
      socket_v2_bnb.SocketGateway_evt_SocketBridge
    WHERE
      evt_block_time >= CURRENT_TIMESTAMP - INTERVAL '1' DAY
  ),
  -- polygon
  socket_v2_polygon AS (
    SELECT
      contract_address,
      evt_tx_hash,
      evt_index,
      evt_block_time,
      evt_block_number,
      amount,
      token,
      toChainId,
      bridgeName,
      sender,
      receiver,
      metadata
    FROM
      socket_v2_polygon.SocketGateway_evt_SocketBridge
    WHERE
      evt_block_time >= CURRENT_TIMESTAMP - INTERVAL '1' DAY
  ),
  -- avalanche_c
  socket_v2_avalanche_c AS (
    SELECT
      contract_address,
      evt_tx_hash,
      evt_index,
      evt_block_time,
      evt_block_number,
      amount,
      token,
      toChainId,
      bridgeName,
      sender,
      receiver,
      metadata
    FROM
      socket_v2_avalanche_c.SocketGateway_evt_SocketBridge
    WHERE
      evt_block_time >= CURRENT_TIMESTAMP - INTERVAL '1' DAY
  ),
  -- fantom
  socket_v2_fantom AS (
    SELECT
      contract_address,
      evt_tx_hash,
      evt_index,
      evt_block_time,
      evt_block_number,
      amount,
      token,
      toChainId,
      bridgeName,
      sender,
      receiver,
      metadata
    FROM
      socket_v2_fantom.SocketGateway_evt_SocketBridge
    WHERE
      evt_block_time >= CURRENT_TIMESTAMP - INTERVAL '1' DAY
  ),
  -- Union all chains
  all_chains AS (
    SELECT
      *,
      'ethereum' as source_chain
    FROM
      socket_v2_ethereum
    UNION ALL
    SELECT
      *,
      'zkevm' as source_chain
    FROM
      socket_v2_zkevm
    UNION ALL
    SELECT
      *,
      'scroll' as source_chain
    FROM
      socket_v2_scroll
    UNION ALL
    SELECT
      *,
      'blast' as source_chain
    FROM
      socket_v2_blast
    UNION ALL
    SELECT
      *,
      'linea' as source_chain
    FROM
      socket_v2_linea
    UNION ALL
    SELECT
      *,
      'mantle' as source_chain
    FROM
      socket_v2_mantle
    UNION ALL
    SELECT
      *,
      'optimism' as source_chain
    FROM
      socket_v2_optimism
    UNION ALL
    SELECT
      *,
      'gnosis' as source_chain
    FROM
      socket_v2_gnosis
    UNION ALL
    SELECT
      *,
      'arbitrum' as source_chain
    FROM
      socket_v2_arbitrum
    UNION ALL
    SELECT
      *,
      'zksync' as source_chain
    FROM
      socket_v2_zksync
    UNION ALL
    SELECT
      *,
      'base' as source_chain
    FROM
      socket_v2_base
    UNION ALL
    SELECT
      *,
      'bnb' as source_chain
    FROM
      socket_v2_bnb
    UNION ALL
    SELECT
      *,
      'polygon' as source_chain
    FROM
      socket_v2_polygon
    UNION ALL
    SELECT
      *,
      'avalanche_c' as source_chain
    FROM
      socket_v2_avalanche_c
    UNION ALL
    SELECT
      *,
      'fantom' as source_chain
    FROM
      socket_v2_fantom
  ),
  prices AS (
    SELECT
      minute,
      blockchain,
      contract_address,
      decimals,
      symbol,
      price
    FROM
      prices.usd
    WHERE
      minute >= CURRENT_TIMESTAMP - INTERVAL '1' DAY
  ),
  calculate_amount_usd AS (
    SELECT
      s.*,
      (s.amount / POW(10, COALESCE(p.decimals, 18))) * p.price AS amount_usd
    FROM
      all_chains s
      LEFT JOIN prices p ON s.source_chain = p.blockchain
      AND s.token = p.contract_address
      AND DATE_TRUNC('minute', s.evt_block_time) = p.minute
  )
SELECT
  *
FROM
  calculate_amount_usd