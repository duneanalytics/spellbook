#!/usr/bin/env python3

import json
	
data = []
# dbt ls --resource-type model > models_made_by_dbt.txt
with open('models_made_by_dbt.txt') as f:
    for line in f:
        data.append(f"{json.loads(line)['config']['schema']}.{json.loads(line)['config']['alias']}")
	
# dbt ls --resource-type seed > seeds_made_by_dbt.txt
with open('seeds_made_by_dbt.txt') as f:
    for line in f:
        data.append(f"{json.loads(line)['config']['schema']}.{json.loads(line)['name']}")
	
with open(r'dbt_models_seeds_names.txt', 'w') as fp:
    fp.write('\n'.join(data))
