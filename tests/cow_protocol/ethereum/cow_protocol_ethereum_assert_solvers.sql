-- Given a list of solvers, when we look at the active solvers, then we should see only 1 per each env and name
WITH unit_test1
    AS (SELECT *
        FROM   {{ ref('cow_protocol_ethereum_solvers' )}}
        WHERE  active = true AND COUNT(address) > 1
        GROUP BY environment, name),
-- Given a list of solvers, , then we should never see a solver who's both true and false for active
    unit_test2
    AS (SELECT count(address)
        FROM   {{ ref('cow_protocol_ethereum_solvers' )}}
        WHERE  COUNT(address) > 1
        GROUP BY address, environment, name)
SELECT *
FROM   (SELECT *
       FROM   unit_test1
       UNION
       SELECT *
       FROM   unit_test2)