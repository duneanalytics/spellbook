DROP FUNCTION loopring.fn_to_uint96;

CREATE OR REPLACE FUNCTION loopring.fn_to_uint96(data bytea) RETURNS double precision AS $$
DECLARE
    result double precision;
    i integer;
BEGIN
    result = 0;
    FOR i IN 1 .. 12
    LOOP
        result = result + get_byte(data, (i-1)) * POWER(2, (96-i*8));
    END LOOP;
    return result;
END; $$
LANGUAGE PLPGSQL;