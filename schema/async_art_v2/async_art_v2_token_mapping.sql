--creating a custom table to map layers to their respective "master" artwork
--the v2 contract is a upgradeable proxy

CREATE TABLE IF NOT EXISTS async_art_v2.token_mapping (
	token_id INT PRIMARY KEY,
  token_type VARCHAR(30),
	master_id INT,
  custom_id VARCHAR(30),
	layer_count INT
 
);
--function to fill table
CREATE OR REPLACE FUNCTION async_art_v2.update_token_mapping () RETURNS void
LANGUAGE plpgsql AS $function$
DECLARE
  r record;
  t_id int;
  type_art VARCHAR(30);
  result int;
  custom_id VARCHAR(30);
  c int;
  

--selecting out of artworks and layers
BEGIN
  FOR r IN select * from 
  (SELECT
  token_id,
  CASE
      WHEN token_id IN      (SELECT 
                            artwork_id 
                            FROM
                            --this is only artworksv2
                            (
                            SELECT 
                            "masterTokenId" as artwork_id
                            , call_success
                            FROM async_art_v2."AsyncArtwork_v2_call_mintArtwork"
                            WHERE call_success ='true'
                            UNION
                            --this is only artworksv1
                            SELECT
                            "artworkTokenId" as artwork_id
                            , call_success
                            FROM async."AsyncArtwork_call_mintArtwork"
                            WHERE call_success ='true'
                            ) as artworks
      ) THEN 'artwork'
      WHEN NOT token_id IN (SELECT 
                            artwork_id 
                            FROM
                            (
                            SELECT 
                            "masterTokenId" as artwork_id
                            , call_success
                            FROM async_art_v2."AsyncArtwork_v2_call_mintArtwork"
                            WHERE call_success ='true'
                            UNION
                            SELECT
                            "artworkTokenId" as artwork_id
                            , call_success
                            FROM async."AsyncArtwork_call_mintArtwork"
                            WHERE call_success ='true'
                            ) as artworks
      ) THEN 'layer'
  END as "token_type"
From 

(
  --all minted tokens in v1 and v2, this includes layers
SELECT 
"tokenId" as token_id

FROM async_art_v2."AsyncArtwork_v2_evt_Transfer"
WHERE "from" = '\x0000000000000000000000000000000000000000'

UNION

SELECT 
"tokenId" as token_id

FROM async."AsyncArtwork_evt_Transfer"
WHERE "from" = '\x0000000000000000000000000000000000000000'

order by 1 asc
) as x) as y order by 1 asc


--loop to fill table and assign custom ID's 
  LOOP

    t_id = r.token_id;
    type_art = r.token_type;


    if r.token_type = 'artwork' then 
      result := r.token_id;
      c := 0;
      custom_id:= CONCAT(r.token_id, '_', c);
    end if;

    if r.token_type = 'layer' then 
      c := c + 1;
      custom_id:= CONCAT(r.token_id, '_', c);
    end if;

    insert into async_art_v2.token_mapping (token_ID, token_type, master_id, custom_id, layer_count)
      VALUES
	  (t_id, type_art,result, custom_id, c)
    ON CONFLICT (token_id) DO UPDATE SET token_id=EXCLUDED.token_id, token_type=EXCLUDED.token_type;
  END LOOP;
END;
$function$;

--insert into automated update schedule
INSERT INTO cron.job (schedule, command)
VALUES ('59 * * * *', $$SELECT async_art_v2.update_token_mapping();$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
