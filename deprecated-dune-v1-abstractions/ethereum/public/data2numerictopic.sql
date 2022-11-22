DROP FUNCTION data2numerictopic(BYTEA, INT, INT);
CREATE OR REPLACE FUNCTION data2numerictopic(data BYTEA, topic INT, decimals INT) RETURNS FLOAT AS $$
BEGIN
RETURN bytea2numeric(decode(SUBSTRING(ENCODE("data",'hex'),(9+(64*"topic")),64),'hex'))/POWER(10, "decimals");
END; $$
LANGUAGE PLPGSQL;