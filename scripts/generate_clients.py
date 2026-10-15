import json
clients=[]
for kind,count in [('s3',50),('azure',10),('gcs',10),('snowflake',50)]:
    for i in range(1,count+1):
        clients.append({'id':f'{kind}_{i:03d}','type':kind,
            'enabled':kind in {'s3','snowflake'},'destination':f'example-{kind}-{i:03d}'})
print(json.dumps(clients,indent=2))
