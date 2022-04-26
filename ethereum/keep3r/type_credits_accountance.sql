CREATE TYPE keep3r.type_credits_accountance AS (
    timestamp TIMESTAMP,
    evt_index INTEGER,
    event VARCHAR,
    job VARCHAR,
    amount float,
    period_credits float
);
