CREATE OR REPLACE VIEW keep3r_network.view_job_credits_log_w_dates AS (
		SELECT COALESCE("timestamp", s.dd) AS timestamp,
			tx_hash,
			COALESCE(evt_index, NULL) AS evt_index,
			COALESCE(event, 'Balance') AS event,
			COALESCE(df.job, s.job) AS job,
			keeper,
			token,
			amount,
			period_credits
		FROM (
				keep3r_network.view_job_credits_log AS df
				RIGHT JOIN (
					SELECT job,
						dd
					FROM ROWS
					FROM (
							generate_series(
								'2021-11-11'::TIMESTAMP,
								now()::TIMESTAMP,
								'1 day'::INTERVAL
							)
						) AS dd
						INNER JOIN (
							SELECT job,
								min("timestamp") AS min_timestamp,
								max("timestamp") AS max_timestamp
							FROM keep3r_network.view_job_credits_log
							GROUP BY job
						) AS df ON dd >= min_timestamp
						AND dd < max_timestamp + '5 days'::INTERVAL
				) AS s ON s.job = df.job
				AND date_trunc('day', s.dd) = date_trunc('day', df.timestamp)
			)
	);
