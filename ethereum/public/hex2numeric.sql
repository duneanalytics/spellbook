DROP FUNCTION IF EXISTS hex2numeric(text);
CREATE OR REPLACE FUNCTION hex2numeric(str TEXT)
RETURNS NUMERIC LANGUAGE plpgsql immutable strict AS $$
DECLARE
    i INT;
    n INT = LENGTH(str)/ 8;
    res DEC = 0;
BEGIN
    str := lpad($1, (n+ 1)* 8, '0');
    FOR i IN 0..n LOOP
        IF i > 0 THEN 
            res := res * 4294967296; 
        END IF;
        res := res + CONCAT('x', SUBSTR(str, i* 8+ 1, 8))::BIT(32)::BIGINT::DEC;
    END LOOP;
    RETURN res;
END $$;