DROP FUNCTION loopring.fn_decode_float_16;

CREATE OR REPLACE FUNCTION loopring.fn_decode_float_16(data bytea) RETURNS double precision AS $$
DECLARE
    exponent integer;
    mantissa integer;
    value integer;
BEGIN
    value = get_byte(data, 0) * 256 + get_byte(data, 1);
    exponent = value / 2048;
    mantissa = value - (exponent * 2048);
    return mantissa * POW(10, exponent);
END; $$
LANGUAGE PLPGSQL;