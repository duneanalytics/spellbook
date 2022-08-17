DROP TABLE IF EXISTS keep3r_network.job_data
;

CREATE TABLE keep3r_network.job_data (
    job,
    job_name
) AS
VALUES (
    '0x5d469e1ef75507b0e0439667ae45e280b9d81b9c' ::varchar,
    'V0-MakerDAOUpkeep' ::varchar),
(
    '0x28937b751050fcfd47fd49165c6e1268c296ba19' ::varchar,
    'V1-MakerDAOUpkeep' ::varchar),
(
    '0x54a8265adc50fd66fd0f961cfcc8b62de0f2b57f' ::varchar,
    'Kasparov' ::varchar
),
(
    '0xe6dd4b94b0143142e6d7ef3110029c1dce8215cb' ::varchar,
    'YearnV2Harvest' ::varchar
),
(
    '0xcd7f72f12c4b87dabd31d3aa478a1381150c32b3' ::varchar,
    'YearnV2Tend' ::varchar
);
