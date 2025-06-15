{{ config(
        schema = 'ethereum',
        tags = ['static'],
        alias = 'network_upgrades',
        post_hook = '{{ expose_spells(\'[
                                        "ethereum"
                                        ]\',
                                        "sector",
                                        "ethereum",
                                        \'["hildobby"]\') }}')
}}

WITH upgrades AS (
        SELECT upgrade_number, release, upgrade, block_number, eips, description, more_details, live, planned, blob_target, blob_limit
        FROM (VALUES
        (1, 'Frontier', 'Frontier', 1, ARRAY[NULL], 'Ethereum initial launch', NULL, true, NULL, NULL, NULL)
        , (2, 'Frontier Thawing', 'Frontier Thawing', 200000, ARRAY[NULL], 'Raised gas limit to enable usage', NULL, true, NULL, NULL, NULL)
        , (3, 'Homestead', 'Homestead', 1150000, ARRAY[2,7,8], 'First stable Ethereum release', NULL, true, NULL, NULL, NULL)
        , (4, 'The DAO Fork', 'The DAO Fork', 1920000, ARRAY[NULL], 'Reverse DAO hack', NULL, true, NULL, NULL, NULL)
        , (5, 'Tangerine Whistle', 'Tangerine Whistle', 2463000, ARRAY[150,158], 'Fix DoS gas issues', NULL, true, NULL, NULL, NULL)
        , (6, 'Spurious Dragon', 'Spurious Dragon', 2675000, ARRAY[155,160,161,170], 'State cleanup and security', NULL, true, NULL, NULL, NULL)
        , (7, 'Byzantium', 'Byzantium', 4370000, ARRAY[140,658,196,197,198,211,214,100,649], 'Privacy and smart contract upgrades', NULL, true, NULL, NULL, NULL)
        , (8, 'Constantinople', 'Constantinople', 7280000, ARRAY[145,1014,1052,1234], 'Efficiency improvements and bomb delay', NULL, true, NULL, NULL, NULL)
        , (9, 'Istanbul', 'Istanbul', 9069000, ARRAY[152,1108,1344,1884,2028,2200], 'Cheaper zk-SNARKs and better compatibility', NULL, true, NULL, NULL, NULL)
        , (10, 'Muir Glacier', 'Muir Glacier', 9200000, ARRAY[2384], 'Delay difficulty bomb', NULL, true, NULL, NULL, NULL)
        , (11, 'Staking Deposit Contract', 'Staking Deposit Contract', 11052984, ARRAY[NULL], 'Enable ETH staking deposits', NULL, true, NULL, NULL, NULL)
        , (12, 'Beacon Chain Genesis', 'Beacon Chain Genesis', 11364381, ARRAY[NULL], 'Launch proof-of-stake chain', NULL, true, NULL, NULL, NULL)
        , (13, 'Berlin', 'Berlin', 12244000, ARRAY[2565,2718,2929,2930], 'Gas cost optimizations', NULL, true, NULL, NULL, NULL)
        , (14, 'London', 'London', 12965000, ARRAY[1559,3198,2529,3541,3554], 'Introduce EIP-1559 and fee burn', NULL, true, NULL, NULL, NULL)
        , (15, 'Altair', 'Altair', 13500000, ARRAY[NULL], 'Beacon Chain sync and penalties', NULL, true, NULL, NULL, NULL)
        , (16, 'Arrow Glacier', 'Arrow Glacier', 13773000, ARRAY[4345], 'Difficulty bomb delay', NULL, true, NULL, NULL, NULL)
        , (17, 'Gray Glacier', 'Gray Glacier', 15050000, ARRAY[5133], 'Final bomb delay before Merge', NULL, true, NULL, NULL, NULL)
        , (18, 'Bellatrix', 'Bellatrix', 15485000, ARRAY[NULL], ' Prepare Beacon Chain for Merge', NULL, true, NULL, NULL, NULL)
        , (19, 'Paris (The Merge)', 'Paris (The Merge)', 15537394, ARRAY[3675,4399], 'Switch to proof of stake', 'https://ethereum.org/en/roadmap/merge/', true, NULL, NULL, NULL)
        , (20, 'Shapella', 'Capella', 17034970, ARRAY[NULL], 'Enable staked ETH withdrawals (consensus)', 'https://ethereum.org/en/staking/withdrawals/', true, NULL, NULL, NULL)
        , (20, 'Shapella', 'Shanghai', 17034970, ARRAY[3651,3855,3860,4895,6049], 'Smart contract upgrades post-Merge (execution)', 'https://ethereum.org/en/staking/withdrawals/', true, NULL, NULL, NULL)
        , (21, 'Dencun', 'Deneb', 19426587, ARRAY[4788,4844,7044,7045,7514], 'Prepares consensus for blob data (consensus)', 'https://ethereum.org/en/roadmap/dencun/', true, NULL, NULL, NULL)
        , (21, 'Dencun', 'Cancun', 19426587, ARRAY[1153,4788,4844,5656,6780,7516], 'Adds EIP-4844 for blob transactions (execution)', 'https://ethereum.org/en/roadmap/dencun/', true, NULL, 3, 6)
        , (22, 'Pectra', 'Prague', 22428718, ARRAY[7702,7840,7251,7685,7549], 'Enhances account abstraction and efficiency, enabling EOAs to delegate control to smart contracts for more flexible transactions.', 'https://ethereum.org/en/roadmap/pectra', true, NULL, 6, 9)
        , (22, 'Pectra', 'Electra', 22428718, ARRAY[2537,2935,6110,7002,7623,7691], 'Validator consolidation & more', 'https://ethereum.org/en/roadmap/pectra', true, NULL, 6, 9)
        , (23, 'Fusaka', 'Fusaka', -1, ARRAY[NULL], '', 'https://ethereum.org/en/roadmap/fusaka/', false, 'Planned for 2025', NULL, NULL)
        , (24, 'Glamsterdam', 'Glamsterdam', -1, ARRAY[NULL], '', 'https://eips.ethereum.org/EIPS/eip-7773', false, 'Planned for 2026', NULL, NULL)
        ) AS x (upgrade_number, release, upgrade, block_number, eips, description, more_details, live, planned, blob_target, blob_limit)
        )

SELECT upgrade_number
, release
, upgrade
, block_number
, time AS block_time
, eips
, description
, more_details
, live
, planned
, blob_target
, blob_limit
FROM upgrades
LEFT JOIN {{ source('ethereum', 'blocks') }} ON block_number=number
ORDER BY upgrade_number DESC