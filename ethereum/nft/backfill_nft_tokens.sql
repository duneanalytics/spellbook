UPDATE
	nft.trades
SET
	nft_project_name = new_names.new_name
FROM
	(
                SELECT
			n.platform,
			n.tx_hash,
			n.trace_address,
			n.evt_index,
			n.trade_id,
			t.name as new_name
		FROM
			nft.trades n
			LEFT JOIN nft.tokens t ON t.contract_address = n.nft_contract_address
		WHERE
			nft_project_name IS NULL
			AND t.name IS NOT NULL
	) as new_names
where
	trades.platform = new_names.platform
	AND trades.tx_hash = new_names.tx_hash
	-- These coalesces are to handle the times these values are null
	-- because in postgres NULL = NULL equals NULL :face_palm:
	AND COALESCE(trades.trace_address, '{-1}') = COALESCE(new_names.trace_address, '{-1}')
	AND COALESCE(trades.evt_index, -1) = COALESCE(new_names.evt_index, -1)
	AND trades.trade_id = new_names.trade_id;
