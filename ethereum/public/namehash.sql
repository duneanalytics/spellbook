-- Enables the use of ENS's namehash function -> https://docs.ens.domains/contract-api-reference/name-processing
-- Python namehash library ->  https://github.com/antonio-mendes/ens-namehash-py/tree/update_library
DROP FUNCTION IF EXISTS namehash(name_to_hash text);
CREATE FUNCTION namehash(name_to_hash text)
    RETURNS bytea
AS $$
    from namehash import namehash
    return namehash(name_to_hash)
$$ LANGUAGE plpython3u IMMUTABLE STRICT;
