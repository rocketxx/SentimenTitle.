import pyspark.sql.types as tp
from pyspark import SparkContext
from pyspark.conf import SparkConf
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, Word2Vec
from pyspark.ml.functions import vector_to_array
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import current_timestamp, from_json



elastic_host = "elasticsearch"

elastic_topic = "news_topic"
elastic_index = "news_topic"



#polarity (0 = negative, 2 = neutral, 4 = positive)
#schema del set di traininng preso da http://help.sentiment140.com/for-students

schema = tp.StructType([
    tp.StructField(name='polarity', dataType=tp.IntegerType(), nullable=True),
    tp.StructField(name='id', dataType=tp.IntegerType(), nullable=True),
    tp.StructField(name='date', dataType=tp.StringType(), nullable=True),
    tp.StructField(name='query', dataType=tp.StringType(), nullable=True),
    tp.StructField(name='user', dataType=tp.StringType(), nullable=True),
    tp.StructField(name='title', dataType=tp.StringType(), nullable=True)
])


def get_spark_session():
    sparkConf= SparkConf() \
                .set("es.nodes", elastic_host) \
                .set("es.port", "9200") \
                .set("spark.app.name", "network_tap") \
                .set("spark.scheduler.mode", "FAIR")
    sc = SparkContext.getOrCreate(conf=sparkConf)
    return SparkSession(sc)

#training.1600000.processed.noemoticon.csv è quello più grande e ci sta un quarto d'ora ad elaborarlo
if __name__ == "__main__":
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    training_set = spark.read.csv('/usr/share/logstash/csv/testdata.manual.2009.06.14.csv',
                                  schema=schema,
                                  header=True,
                                  sep=',')
    training_set.groupBy("polarity").count().show()
    training_set.show(5)

    # define stage 1: tokenize the tweet text
    stage_1 = RegexTokenizer(inputCol='title', outputCol='tokens', pattern='\\W')

    # define stage 2: remove the stop words
    stage_2 = StopWordsRemover(inputCol='tokens', outputCol='filtered_words')

    # define stage 3: create a word vector of the size 100
    stage_3 = Word2Vec(inputCol='filtered_words', outputCol='vector', vectorSize=100)

    # define stage 4: Logistic Regression Model
    model = LogisticRegression(featuresCol='vector', labelCol='polarity')

    # setup the pipeline
    pipeline = Pipeline(stages=[stage_1, stage_2, stage_3, model])

    # fit the pipeline model with the training data
    pipelineFit = pipeline.fit(training_set)

    # Show some data (opzionale, si potrebeb rimuovere per migliorare le performance)
    print("*****************Pipeline fit *****************************")
    print(pipelineFit)
    print("*****************Accuracy*****************************")
    modelSummary = pipelineFit.stages[-1].summary
    print(modelSummary.accuracy)
    print("*****************model*****************************")
    print(pipelineFit.explainParams())
    print("**********************************************")
    print("*****************example output*****************************")
    pipelineFit.transform(training_set)\
                .withColumn("@timestamp", current_timestamp()) \
                .withColumn("probability", vector_to_array("probability")) \
                .select("@timestamp", "title", "polarity", "prediction", "probability") \
                .show(3)
    print("**********************************************")



#1) predict prima di inviare, lo ritrasforma in pandas
#2) predict prende un pandas, lo converte in spark Df, applica la fit, lo ritrasforma in pandas
def predict(df: DataFrame) -> DataFrame:
    # necessario per i dati che arrivano da kafka
    df = df.selectExpr("CAST(value AS STRING)") \
           .select(from_json("value", schema=schema).alias("data")) \
           .select("data.*")
    df = pipelineFit.transform(df)  # passo spark df perchè transform vuole un spark df
    # aggiunge la colonna timestamp e sistema la colonna probability. Poi seleziona solo le colonne utili
    df = df.withColumn("@timestamp", current_timestamp()) \
            .withColumn("probability", vector_to_array("probability")) \
            .select("@timestamp", "title", "polarity", "prediction", "probability")
    return df


dataStream = spark\
            .readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'kafkaserver:9092') \
            .option('subscribe', 'news_topic') \
            .option('startingOffsets', 'earliest')\
            .load()

dataStream = predict(dataStream)

dataStream.writeStream\
          .option("checkpointLocation", "./checkpoints") \
          .format("es") \
          .start(elastic_index + "/_doc")\
          .awaitTermination()
