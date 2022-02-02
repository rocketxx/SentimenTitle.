from newsapi import NewsApiClient
import json
import pandas as pd
import os

key = 0
# gia cercate oltre a quelle sotto: tesla, spaceX
# ["Google","Apple","Bitcoin","Meta","IBM","Amazon","Cryptocurrency"]  
# ["Snapchat","Whatsapp","Instagram", "TikTok"] 
while(True):
    list_key_search = ["Google","Apple","Bitcoin","Meta","IBM","Amazon","Cryptocurrency"]
    size = len(list_key_search) #il prof di ingegneria del software approva questa usanza 

    newsapi = NewsApiClient(api_key='cba1ed0592e44116ae5a6eea16e0d42b')

# q è Keywords o frase da ricercare
# qui l'unico problema è che puoi raccogliere a scrocco solo news ultimo mese. nescia i soddi e avrai anche lo storico
    articles = newsapi.get_everything(q=list_key_search[key],
                                        sources='bbc-news,the-verge',
                                        domains='bbc.co.uk,techcrunch.com',
                                        from_param='2021-20-12',
                                        to='2021-20-01',
                                        language='en',
                                        sort_by='relevancy',
                                        page=2)



#creo json
    with open('./logstash/csv/data.json', 'w') as news_file:
        json.dump(articles, news_file)

    with open('./logstash/csv/data.json','r') as f:
        data = json.loads(f.read())
#skippo i campi che non mi interessano e salvo quelli che mi servono
    df_nested_list = pd.json_normalize(data, record_path =['articles'])

#elimino i campi che non voglio o che al momento non servono, cosi è facilmente modificabile in futuro
    
    newdf = df_nested_list.drop(['description','url', 'content','urlToImage','author','source.id','source.name','publishedAt','description'], axis=1)
    print(newdf)

    newdf.to_csv('./logstash/csv/file2.csv', index = None)#traduco e salvo in csv

#così non spammo json e mi resta la directory pulita
    os.remove("./logstash/csv/data.json")

#copio le righe del file nuovo nel file "vecchio" dove ci sono tutti gli articoli già raccolti
    def concatenate_csv(path1,path2):
        file1 = open(path1, "a")
        file2 = open(path2, "r",errors='ignore')  # risolve il bug dell'encodig che dà errore con caratteri cinesi
        next(file2) #skippo header del file due    
        for line in file2:  
            file1.write(line)

        file1.close()
        file2.close()



    concatenate_csv('./logstash/csv/news.csv','./logstash/csv/file2.csv')
    os.remove("./logstash/csv/file2.csv") #elimino csv cosi non c'è bisogno di cambiare il path per ogni esecuzione del get_news script
    key+=1  
    if(key>size-1):    #limite delle keywords
        break