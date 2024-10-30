'''Processamento em Batch
Definição: Processamento de grandes volumes de dados em lotes. Os dados são coletados e processados em intervalos.
API: Use o Spark DataFrame ou RDD (Resilient Distributed Dataset).
Exemplo: Leitura de arquivos CSV, JSON ou Parquet, transformações de dados, agregações e escrita em sistemas de armazenamento como HDFS ou S3.
Processamento em Stream
Definição: Processamento contínuo de dados em tempo real. Os dados são processados assim que chegam.
API: Use o Spark Structured Streaming, que permite construir aplicações de streaming com a mesma API usada para batch.
Exemplo: Monitoramento de dados de redes sociais, logs de servidores, ou eventos de IoT.
Ferramentas e Conceitos Comuns
DataFrames: Estrutura de dados tabular que facilita operações.
Transformações: Operações como map, filter, e reduce.
Ações: Operações que retornam resultados, como count, collect, ou write.
Janelas: No streaming, você pode usar janelas para agrupar eventos em períodos de tempo definidos.
Dicas
Tuning: Ajuste a configuração do Spark para otimizar o desempenho, especialmente para tarefas de processamento em larga escala.
Checkpointing: No streaming, use checkpointing para garantir a tolerância a falhas.
Testes: Faça testes em pequenas amostras de dados antes de escalar.'''

###Processamento de dados em batch
from pyspark.sql import SparkSession

# Criação da sessão do Spark
spark = SparkSession.builder \
    .appName("Batch Processing Example") \
    .getOrCreate()

# Leitura de um arquivo CSV
df = spark.read.csv("path/to/your/data.csv", header=True, inferSchema=True)

# Exibição dos dados
df.show()

# Transformações: Filtrando dados
filtered_df = df.filter(df['age'] > 21)

# Agregações: Contando quantas pessoas por cidade
result_df = filtered_df.groupBy("city").count()

# Escrita dos resultados em um arquivo Parquet
result_df.write.parquet("path/to/output/result.parquet")

# Encerrando a sessão
spark.stop()


# Processamento de dados em Stream
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window

# Criação da sessão do Spark
spark = SparkSession.builder \
    .appName("Stream Processing Example") \
    .getOrCreate()

# Leitura de dados de um diretório em modo de stream
streaming_df = spark.readStream \
    .option("header", "true") \
    .csv("path/to/your/streaming_data/")

# Transformação: Filtrando dados
filtered_stream = streaming_df.filter(col("event_type") == "purchase")

# Agregação: Contando eventos por janela de tempo
result_stream = filtered_stream.groupBy(
    window(col("timestamp"), "10 minutes"),
    col("item")
).count()

# Escrita dos resultados em console
query = result_stream.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

# Aguardando a conclusão do streaming
query.awaitTermination()

# Encerrando a sessão (não será alcançado até que o streaming pare)
spark.stop()