CREATE SCHEMA IF NOT EXISTS ovm1;

DROP TABLE ovm1.user_addresses_weekly_active_cohorts;

CREATE TABLE IF NOT EXISTS ovm1.user_addresses_weekly_active_cohorts (
	user_address bytea,
	week_active timestamptz,
	first_week_cohort timestamptz,
	num_weeks_elapsed INT,
	num_transactions INT,
		PRIMARY KEY(user_address, week_active)
);

BEGIN;
TRUNCATE ovm1.user_addresses_weekly_active_cohorts;

COPY ovm1.user_addresses_weekly_active_cohorts (user_address, week_active, first_week_cohort, num_weeks_elapsed, num_transactions) FROM stdin;
\\xac1e8b385230970319906c03a1d8567e3996d1d5	2021-06-21T00:00:00+00:00	2021-06-21T00:00:00+00:00	0	403
\\x0000000000000000000000000000000000000000	2021-06-21T00:00:00+00:00	2021-06-21T00:00:00+00:00	0	154
\\xd16d160ff95e492371eac5da4bf255806c088990	2021-06-21T00:00:00+00:00	2021-06-21T00:00:00+00:00	0	47
\\x9a027b64d6fc4a77e34ae40f5e6f9310c457737f	2021-06-21T00:00:00+00:00	2021-06-21T00:00:00+00:00	0	23
\\x3838137a3d54ae725ed762092101d4b738759b52	2021-06-21T00:00:00+00:00	2021-06-21T00:00:00+00:00	0	21
\\x51a59a3a9a31f3a7d9d1675b18d0369a52dfc08c	2021-06-21T00:00:00+00:00	2021-06-21T00:00:00+00:00	0	12
\\x7a3d05c70581bd345fe117c06e45f9669205384f	2021-06-21T00:00:00+00:00	2021-06-21T00:00:00+00:00	0	12
\\x5ce259aedcd14038ad2c35ecaa7b6b2974d339ac	2021-06-21T00:00:00+00:00	2021-06-21T00:00:00+00:00	0	11
\\xa27b484162492d86631c5dd868afd426db39876c	2021-06-21T00:00:00+00:00	2021-06-21T00:00:00+00:00	0	10
\\xac63b3a69604925dadf2abd13877d7a4a7113308	2021-06-21T00:00:00+00:00	2021-06-21T00:00:00+00:00	0	10
\\x716b58046f5feee07aa75eb90a0339a5ae406964	2021-06-21T00:00:00+00:00	2021-06-21T00:00:00+00:00	0	10
\\xa4169b535607e347a6807d8cf7276e03e2043d78	2021-06-21T00:00:00+00:00	2021-06-21T00:00:00+00:00	0	9
\.

COMMIT;

CREATE INDEX IF NOT EXISTS ovm1_uuser_addresses_weekly_active_cohorts_user_address_week_active_idx ON ovm1.user_addresses_weekly_active_cohorts (user_address,week_active);
