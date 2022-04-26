CREATE TYPE dune_user_generated.type_credits_accountance AS (
    timestamp TIMESTAMP,
    evt_index INTEGER,
    event VARCHAR,
    job VARCHAR,
    amount float,
    period_credits float
);
