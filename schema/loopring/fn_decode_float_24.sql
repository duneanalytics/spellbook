DROP FUNCTION loopring.fn_decode_float_24;

CREATE OR REPLACE FUNCTION loopring.fn_decode_float_24(data bytea) RETURNS double precision AS $$
DECLARE
    exponent integer;
    mantissa integer;
    value integer;
BEGIN
    value = get_byte(data, 0) * 65536 + get_byte(data, 1) * 256 + get_byte(data, 2);
    exponent = value / 524288;
    mantissa = value - (exponent * 524288);
    return mantissa * POW(10, exponent);
END; $$
LANGUAGE PLPGSQL;