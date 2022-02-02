import json
import pandas as pd
from urllib.request import urlopen
#country = puoi mettere anche it e li prende in italiano
url = "https://newsapi.org/v2/top-headlines?country=us&apiKey=cba1ed0592e44116ae5a6eea16e0d42b"
#df = pd.read_json(url)

response = urlopen(url)
  
# storing the JSON response 
# from url in data
data_json = json.loads(response.read())

with open('./logstash/csv/news_data.json', 'w') as news_file:
    json.dump(data_json, news_file)

with open('./logstash/csv/news_data.json','r') as f:
    data = json.loads(f.read())

df_nested_list = pd.json_normalize(data, record_path =['articles'])
newdf = df_nested_list.drop(['description', 'url', 'content','urlToImage','author','source.id','source.name','publishedAt','description'], axis=1)
print(newdf)
newdf.to_csv('./logstash/csv/news_data.csv', index = None)

