DROP FUNCTION loopring.fn_to_uint32;

CREATE OR REPLACE FUNCTION loopring.fn_to_uint32(data bytea) RETURNS integer AS $$
BEGIN
    return (get_byte(data, 0) & 127) * 16777216 + get_byte(data, 1) * 65536  + get_byte(data, 2) * 256 + get_byte(data, 3);
END; $$
LANGUAGE PLPGSQL;