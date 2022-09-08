import json
import pandas as pd

with open('tokens.json') as f:
    d = json.load(f)
    print(d)

df = pd.read_json('tokens.json')
df=df.drop(['href'], axis=1)
df['address']=df['address'].str.lower()
df.to_csv('tokens.csv', index=False)