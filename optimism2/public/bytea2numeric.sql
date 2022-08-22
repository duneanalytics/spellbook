-- DROP FUNCTION bytea2numeric(bytea, boolean, text);
CREATE OR REPLACE FUNCTION bytea2numeric(a bytea, signed boolean = true, byteorder text = 'big')
    RETURNS numeric
AS $$
    return int.from_bytes(a, byteorder=byteorder, signed=signed)
$$ LANGUAGE plpython3u IMMUTABLE STRICT;
