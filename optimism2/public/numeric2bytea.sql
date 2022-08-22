-- DROP FUNCTION numeric2bytea(numeric, integer, boolean, text);
CREATE OR REPLACE FUNCTION numeric2bytea (a numeric, _length integer = 32, signed boolean = true, byteorder text = 'big')
  RETURNS bytea
AS $$
    return int(a).to_bytes(_length, byteorder=byteorder, signed=signed)
$$ LANGUAGE plpython3u IMMUTABLE STRICT;
