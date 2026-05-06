
# SentimenTitle

> Pipeline Big Data per la **sentiment analysis in tempo reale** di titoli di notizie, basata su Apache Kafka, Apache Spark (MLlib), Elasticsearch e Kibana — interamente orchestrata con Docker Compose.

## Panoramica

Il progetto raccoglie titoli di articoli da fonti giornalistiche (BBC News, The Verge, TechCrunch) tramite la [NewsAPI](https://newsapi.org/), li fa transitare attraverso una pipeline di streaming e applica un modello di classificazione del sentimento addestrato su un dataset di 1.6M di tweet ([Sentiment140](http://help.sentiment140.com/for-students)). I risultati vengono indicizzati in Elasticsearch e visualizzati in una dashboard Kibana.

## Architettura

```
NewsAPI ──► get_news.py ──► CSV
                              │
                              ▼
                    Logstash (ingest CSV)
                              │
                              ▼
                     Kafka (news_topic)
                              │
                              ▼
                Spark Structured Streaming
                   + MLlib (Logistic Regression)
                              │
                              ▼
                       Elasticsearch
                              │
                              ▼
                     Kibana (Dashboard)
```

### Flusso dei dati

1. **Raccolta** — Lo script `get_news.py` interroga la NewsAPI per keyword (Google, Apple, Bitcoin, Tesla, Meta, IBM, Amazon, Cryptocurrency) e salva i titoli in un file CSV.
2. **Ingestione** — Logstash legge il CSV, estrae il campo `title` e lo pubblica sul topic Kafka `news_topic`.
3. **Elaborazione** — Spark Structured Streaming consuma i messaggi da Kafka, applica una pipeline ML (tokenizzazione, rimozione stopwords, Word2Vec, Logistic Regression) e produce una predizione di sentimento (negativo / neutro / positivo).
4. **Indicizzazione** — I risultati vengono scritti automaticamente su Elasticsearch tramite il connettore `elasticsearch-spark`.
5. **Visualizzazione** — Kibana mostra i risultati in una dashboard interattiva con distribuzione del sentimento, trend temporali e dettagli per keyword.

## Pipeline ML (Spark MLlib)

Il modello di sentiment analysis utilizza una pipeline a 4 stadi:

| Stadio | Componente         | Descrizione                                     |
|--------|--------------------|-------------------------------------------------|
| 1      | RegexTokenizer     | Suddivisione del testo in token                 |
| 2      | StopWordsRemover   | Rimozione delle stopwords inglesi                |
| 3      | Word2Vec           | Vettorizzazione (embeddings a 100 dimensioni)    |
| 4      | LogisticRegression | Classificazione del sentimento (0/2/4)           |

Il training avviene sul dataset Sentiment140, dove la polarità è codificata come: 0 = negativo, 2 = neutro, 4 = positivo.

## Prerequisiti

- [Docker](https://docs.docker.com/get-docker/) e [Docker Compose](https://docs.docker.com/compose/install/)
- Una API key gratuita da [NewsAPI](https://newsapi.org/) (per la raccolta delle notizie)

## Avvio rapido

```bash
# 1. Clonare il repository
git clone https://github.com/rocketxx/SentimenTitle..git
cd SentimenTitle.

# 2. Avviare l'intera pipeline
docker-compose up --build
```

Una volta avviati i container, i servizi saranno disponibili ai seguenti indirizzi:

| Servizio         | URL                     |
|------------------|-------------------------|
| Kibana           | http://localhost:5601    |
| Kafka UI         | http://localhost:8080    |
| Spark UI         | http://localhost:4040    |

Elasticsearch (porta 9200) e Kafka (porta 9092) sono esposti solo internamente alla rete Docker.

### Raccolta notizie

Prima di avviare la pipeline, è necessario popolare il CSV con i titoli delle notizie:

```bash
# Inserire la propria API key in spark/get_news.py, poi:
python spark/get_news.py
```

Lo script raccoglie titoli per ciascuna keyword configurata e li accoda nel file `logstash/csv/news.csv`.

## Struttura del progetto

```
SentimenTitle./
├── docker-compose.yml              # Orchestrazione di tutti i servizi
├── kafka/
│   ├── Dockerfile                  # Kafka + Zookeeper su OpenJDK 8
│   ├── conf/                       # Configurazioni Kafka e Zookeeper
│   └── setup/                      # Binari Kafka 2.13-2.7.0
├── logstash/
│   ├── Dockerfile                  # Logstash 7.11.2
│   ├── pipeline/*.conf             # Pipeline: CSV → Kafka (news_topic)
│   └── csv/
│       ├── news.csv                # Titoli raccolti da NewsAPI
│       └── testdata.manual.2009.06.14.csv  # Dataset Sentiment140 (training)
├── spark/
│   ├── Dockerfile                  # PySpark 3.1.1 + dipendenze ML
│   ├── spark_ML.py                 # Pipeline ML + Structured Streaming
│   ├── get_news.py                 # Script di raccolta notizie da NewsAPI
│   ├── backup.py                   # Versione commentata con mapping ES opzionale
│   └── test.py                     # Script di test per le top headlines
├── kibana/
│   ├── Dockerfile                  # Kibana 7.12.1
│   ├── kibana.yml                  # Configurazione (punta a Elasticsearch)
│   └── Final_Dashboard.json        # Dashboard pre-configurata (importabile)
└── Presentazione/
    └── SentimenTitle.ipynb          # Notebook Jupyter di presentazione
```

## Stack tecnologico

| Componente      | Versione       | Ruolo                                        |
|-----------------|----------------|----------------------------------------------|
| Apache Kafka    | 2.7.0          | Message broker (streaming dei titoli)         |
| Apache Spark    | 3.1.1          | Structured Streaming + MLlib                  |
| Logstash        | 7.11.2         | Ingestione CSV → Kafka                        |
| Elasticsearch   | 7.12.1         | Indicizzazione e ricerca dei risultati        |
| Kibana          | 7.12.1         | Dashboard di visualizzazione                  |
| Kafka UI        | latest         | Interfaccia web per monitorare topic e broker |
| NewsAPI         | —              | Fonte dati (titoli di notizie)                |

## Dashboard Kibana

Il file `kibana/Final_Dashboard.json` contiene una dashboard pre-configurata importabile direttamente da Kibana (Management → Saved Objects → Import). La dashboard include visualizzazioni sulla distribuzione del sentimento e sulla polarità dei titoli analizzati.

## Note

- Il dataset di training (`testdata.manual.2009.06.14.csv`) è la versione ridotta di Sentiment140. Per risultati migliori è possibile utilizzare il dataset completo da 1.6M di record (`training.1600000.processed.noemoticon.csv`), tenendo conto che il tempo di training aumenta significativamente.
- La API key NewsAPI inclusa nel codice è a scopo dimostrativo. Si consiglia di sostituirla con la propria.
