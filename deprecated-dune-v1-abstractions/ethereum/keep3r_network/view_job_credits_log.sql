CREATE OR REPLACE VIEW keep3r_network.view_job_credits_log AS (
	WITH work_evt AS (
		
				SELECT evt_block_time AS timestamp,
					'0x' || encode(evt_tx_hash, 'hex') AS tx_hash,
					evt_index,
					'KeeperWork' AS event,
					'0x' || encode(contract_address, 'hex') AS keep3r,
					'0x' || encode(_job, 'hex') AS job,
					'0x' || encode(_keeper, 'hex') AS keeper,
					'0x' || encode(_credit, 'hex') AS token,
					_amount / 1e18 AS amount
				FROM (
						SELECT *
						FROM keep3r_network."Keep3r_evt_KeeperWork"
						UNION
						SELECT *
						FROM keep3r_network."Keep3r_v2_evt_KeeperWork"
					) keep3rWork
				WHERE _credit = '\x1ceb5cb57c4d4e2b2433641b95dd330a33185a44'
			),
			reward_evt AS (
				SELECT to_timestamp("_rewardedAt") AS timestamp,
					'0x' || encode(evt_tx_hash, 'hex') AS tx_hash,
					evt_index,
					'CreditsReward' AS event,
					'0x' || encode(contract_address, 'hex') AS keep3r,
					'0x' || encode(_job, 'hex') AS job,
					NULL AS keeper,
					'0x1ceb5cb57c4d4e2b2433641b95dd330a33185a44' AS token,
					"_currentCredits" / 1e18 AS amount,
					"_periodCredits" / 1e18 AS period_credits
				FROM (
						SELECT *
						FROM keep3r_network."Keep3r_evt_LiquidityCreditsReward"
						UNION
						SELECT *
						FROM keep3r_network."Keep3r_v2_evt_LiquidityCreditsReward"
					) rewards
			)
			SELECT *,
				NULL AS period_credits
			FROM work_evt

			UNION

			SELECT *
			FROM reward_evt

			UNION
			SELECT timestamp,
				tx_hash,
				evt_index,
				event,
				keep3r,
				job,
				null as keeper,
				'0x1ceb5cb57c4d4e2b2433641b95dd330a33185a44' AS token,
				null as amount,
				null as period_credits
			FROM keep3r_network.view_job_migrations

);
