# Databricks notebook source
# MAGIC %md
# MAGIC # Etapa 3 - Análise de dados
# MAGIC Leitura das bases do star-schema e respostas das perguntas elaboradas no objetivo

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

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3) Qual a categoria de produtos mais presente?
# MAGIC

# COMMAND ----------

df <- products %>% dplyr::group_by(category) %>% dplyr::count()
dplyr::arrange(df, desc(n))


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC O mercado é dominado pelos produtos de Casa e Cozinha, Eletronicos e Computadores.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4) Qual a média, mediana, mínima e máxima do preço dos produtos das principais categorias?
# MAGIC

# COMMAND ----------

df <- products %>%
  dplyr::group_by(category) %>%
  dplyr::summarise(media = mean(actual_price), mediana=median(actual_price),minimo = min(actual_price),maximo = max(actual_price), n = dplyr::n())
  head(dplyr::arrange(df, desc(n)),4)


# COMMAND ----------

# MAGIC %md
# MAGIC O maior preço foi da categoria de Electronics, por 9373 reais, e o menor da categoria de computadores por 2,61 reais.
# MAGIC
# MAGIC Lembrete: Os valores originais da base estavam em rúpias indianas e foram convertidos para Reais com base na cotação do dia 01/07/2024 no notebook create_schema.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5) Qual a porcentagem de produtos com nota de avaliação suficientemente confiável?
# MAGIC
# MAGIC Pelo TLC, um tamanho de amostra 30 é o suficiente para a aproximação da distribuição normal. Aqui pode-se imaginar que se o rating_count for maior que 30 as notas são confiáveis. 

# COMMAND ----------

df <- products %>%
  dplyr::mutate(nota_maior_30 = ifelse(rating_count>=30,T,F)) %>%
  dplyr::group_by(nota_maior_30) %>% dplyr::count()

df$porcentagem <- round((df$n/sum(df$n))*100,2)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Quase 94% dos produtos possuem nota com quantidade suficiente de avaliações para confiabilidade da nota

# COMMAND ----------

# MAGIC %md