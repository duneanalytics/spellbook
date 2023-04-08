{% docs sandwich_attackers %}

## Known sandwich attacker addresses

The logic to identify a sandwich attacker is as follows:

1. Two separate trades, t1 and t2 in no particular order, in the same block initiated by the same address
2. Both trades initiated on the same project
3. Token bought in t1 = token sold in t2
4. Token sold in t1 = token bought in t2
5. Amount bought in t1 = Amount sold in t2
6. Amount sold in t1 < amount bought in t2 (excluded)
7. (index of t1 >= index of t2 + 2) or (index of t2 >= index of t1 + 2)
8. Exclude uniswap v2 router address since this gets included as a false positive for some reason

This logic should work to include both buy-first and less common sell-first sandwiches
By excluding point 6, we also include sandwich attacks that made a loss
By using '>=' instead of just '=' in point 7, we can also include sandwich attackers that are perhaps not using flashbots and not tightly bundling their transactions

{% enddocs %}
