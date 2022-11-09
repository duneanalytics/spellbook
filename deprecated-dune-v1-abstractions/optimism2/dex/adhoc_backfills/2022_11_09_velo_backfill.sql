SELECT dex.insert_velodrome(
    '2022-11-01'::timestamptz,
    now(),
    0,
    (SELECT MAX(number) FROM optimism.blocks where time < now() - interval '20 minutes')
)
