CREATE OR REPLACE VIEW dune_user_generated.view_job_log AS (
	(select
	timestamp,
	tx_hash,
	evt_index,
	event,
	keep3r,
	job,
	null as keeper,
	token,
	amount,
	null as period_credits
	from dune_user_generated.view_job_liquidity_log

	union all
	select * from dune_user_generated.view_job_credits_log

	)
	order by timestamp, evt_index
	
);
