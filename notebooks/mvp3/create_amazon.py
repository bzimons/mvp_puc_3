# Databricks notebook source
# MAGIC %md
# MAGIC # Etapa 1 - Criação da base de dados em formato parquet
# MAGIC Essa etapa de carregamento é oferecida pelo próprio databricks community edition. Aqui mantenho o comentário original. O Databricks utiliza do *Pyspark* para a leitura e gravação dos dados:

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/amazon.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# COMMAND ----------

permanent_table_name = "amazon__csv"
df.write.format("parquet").saveAsTable(permanent_table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC O comando acima salva o dataset original em parquet. Ele aparentemente, uma vez executado, não necessita executar novamente. Porém mantenho aqui o comando para deixar claro como foi escrito o DF em parquet no Databricks.