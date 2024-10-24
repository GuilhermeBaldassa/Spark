# 1. Configurando o PySpark
# Antes de começar, certifique-se de ter a sessão do Spark criada:

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ExemplosBasicos").getOrCreate()

# 2. Criando um DataFrame
# Você pode criar um DataFrame a partir de uma lista de dados:

data = [("Alice", 1), ("Bob", 2), ("Cathy", 3)]
columns = ["Nome", "Idade"]

df = spark.createDataFrame(data, columns)
df.show()

# 3. Lendo um arquivo CSV
# Para ler um arquivo CSV:

df_csv = spark.read.csv("./files/dados.csv", header=True, inferSchema=True)
df_csv.show()

# 4. Selecionando colunas
# Você pode selecionar colunas específicas de um DataFrame:

df.select("Nome").show()

# 5. Filtrando dados
# Para filtrar linhas com base em uma condição:

df.filter(df.Preço > 1).show()

# 6. Adicionando uma nova coluna
# Você pode adicionar uma nova coluna com base em uma operação em outras colunas:

from pyspark.sql.functions import col

df = df.withColumn("novo_preco", col("Preço") * 0.20)
df.show()

# 7. Agrupando dados
# Para agrupar dados e calcular agregações:

df.groupBy("novo_preco").count().show()

# 8. Ordenando dados
# Para ordenar um DataFrame:

df.orderBy("Categoria").show()

# 9. Salvando um DataFrame
# Para salvar um DataFrame em um arquivo CSV:

df.write.csv("./arquivo_saida.csv", header=True)

# 10. Finalizando a sessão
# Não se esqueça de parar a sessão quando terminar:

spark.stop()
