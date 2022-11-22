CREATE OR REPLACE FUNCTION hex2utf(hex text)
RETURNS TEXT
AS $$
    return bytes.fromhex(hex).decode('utf-8')
$$ LANGUAGE plpython3u IMMUTABLE STRICT;