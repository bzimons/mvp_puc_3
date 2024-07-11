# Databricks notebook source
# MAGIC %md
# MAGIC # Etapa 3 - Análise de dados
# MAGIC Leitura das bases do star schema e respostas das perguntas elaboradas no objetivo

# COMMAND ----------

#bibliotecas 
library(dplyr)
library(SparkR)

# COMMAND ----------

#leitura das bases
fato <- read.df("/amazon_star_schema/fato.parquet", "parquet")
users <- read.df("/amazon_star_schema/users.parquet", "parquet")
products <- read.df("/amazon_star_schema/products.parquet", "parquet")
reviews <- read.df("/amazon_star_schema/reviews.parquet", "parquet")

# COMMAND ----------

# transformação do DF Spark para R
fato <- as.data.frame(fato)
users <- as.data.frame(users)
products <- as.data.frame(products)
reviews <- as.data.frame(reviews)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1) Existem usuários que compraram mais de um produto?

# COMMAND ----------

#usuarios que deram mais de um review para produtos (ou seja, compraram produtos diferentes)

usercounted <- fato %>% dplyr::group_by(user_id) %>% dplyr::count() 
user_fil <- usercounted[usercounted$n>1,]
print(paste(nrow(user_fil)," usuários escreveram reviews para mais de um produto"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2) As categorias dos usuários que compraram o mesmo produto são as mesmas?

# COMMAND ----------

# usuarios que compraram mais de um produto:
df <- fato %>% dplyr::filter(user_id %in% user_fil$user_id)

df <- merge(df,products,by="product_id", all.x=T)
df_gp <- df %>% dplyr::group_by(user_id,category) %>% dplyr::count()
df_gp <- df_gp %>% dplyr::group_by(user_id,n) %>% dplyr::count()

x1 <- round(sum(df_gp$n>=2)/nrow(df_gp),4)*100 # compraram na mesma categoria
x2 <- round(sum(df_gp$n<2)/nrow(df_gp),4)*100 # compraram em categorias diferentes

print(paste(x1,"% compraram na mesma categoria e ",x2,"% em categorias diferentes."))

# COMMAND ----------

# MAGIC %md
# MAGIC