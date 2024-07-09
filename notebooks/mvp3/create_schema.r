# Databricks notebook source
library(dplyr)
library(SparkR)

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/amazon.csv"
file_type = "csv"
amazon <- read.df("/FileStore/tables/amazon.csv", source = "csv", header="true", inferSchema = "true")
amazon <- drop(amazon, c("review_content","about_product")) #necessario para trazer o DF para DF em R e não em SparkR
head(amazon)

# COMMAND ----------

class(amazon)

# COMMAND ----------

head(amazon[amazon$product_id=='B0BNVBJW2S',])
# o problema é notado aqui. por algum motivo, algumas linhas se misturam na propria leitura do DF. neste caso, vemos que o product name "entra dentro" das categorias. e fica tudo bagunçado.

# COMMAND ----------

amaz <- as.data.frame(amazon)
class(amaz)

# COMMAND ----------

# MAGIC %md
# MAGIC Agora que o DF é em R, podemos fazer a separação:

# COMMAND ----------

#criando a df_users, em que separamos usuarios,reviews e categorias de dentro da base original

i=1
df_users = data.frame(user_id = strsplit(as.character(amaz$user_id[i]),',')[[1]],
                      user_name = strsplit(as.character(amaz$user_name[i]),',')[[1]],
                      review_id = strsplit(as.character(amaz$review_id[i]),',')[[1]],
                 review_title = strsplit(as.character(amaz$review_title[i]),',')[[1]],
                 product_id = amaz$product_id[i],
                 category = strsplit(as.character(amaz$category[i]),'\\|')[[1]][1])


for(i in 2:nrow(amaz)){
  tryCatch({
dfc = data.frame(user_id = strsplit(as.character(amaz$user_id[i]),',')[[1]],
                 user_name = strsplit(as.character(amaz$user_name[i]),',')[[1]],
                 review_id = strsplit(as.character(amaz$review_id[i]),',')[[1]],
           review_title = strsplit( gsub("(,[a-zA-Z]+)|(,-)|(,[0-9])|(,à)","~\\1",as.character(amaz$review_title[i])), "~" )[[1]],
           product_id = amaz$product_id[i],
           category = strsplit(as.character(amaz$category[i]),'\\|')[[1]][1])
# print(i)
df_users = rbind(df_users,dfc)
},error=function(e){cat("ERROR :",conditionMessage(e),i, "\n")})
}


# COMMAND ----------

# MAGIC %md
# MAGIC Criando as Tabelas FATO, PRODUCTS, USERS e REVIEWS

# COMMAND ----------

# checando os produtos iguais, mas product_id diferentes:

prod_diff <- df_users %>% 
  group_by(user_id,user_name,review_id) %>%
  mutate(id_order = row_number())

prod_diff <- prod_diff %>% filter(id_order==1) %>% select(-c(id_order))

# confirmando se temos agora produtos unicos com usuarios e reviews unicos:
nrow(prod_diff)
nrow(prod_diff %>% group_by(user_id,review_id,product_id) %>% count())

df_users <- prod_diff

# 8616 produtos que conseguimos separar os reviews dos usuários 

fato = df_users[,c('user_id','review_id','product_id')]
users = df_users[,c('user_id','user_name')]
reviews = df_users[,c('review_id','review_title')]
products0 = amaz[,(names(amaz) %in% c("product_id", "product_name", "discounted_price", "actual_price","discount_percentage", "rating"))]



categories <- df_users %>% group_by(product_id,category) %>% count() %>% select(-c(n))

products <- merge(products0,categories,by="product_id")

products <- products %>% filter(product_id %in% df_users$product_id)


fato <- distinct(fato)
users <- distinct(users)
reviews <- distinct(reviews)
products <- distinct(products)


products$actual_price <- gsub('₹','',products$actual_price)
products$discounted_price <- gsub('₹','',products$discounted_price)

#Rupias Indianas Para Reais: (valor em 01/07/2024)
products$actual_price <-round(as.numeric(gsub(",","",products$actual_price))*0.067,2)
products$discounted_price <-round(as.numeric(gsub(",","",products$discounted_price))*0.067,2)


# COMMAND ----------

products0[products0$product_id=='B0BNVBJW2S',]

# COMMAND ----------

amaz[amaz$product_id=='B0BNVBJW2S',]

# COMMAND ----------

# MAGIC %md
# MAGIC o problema do valor estar deslocado é o mesmo desde a base inicial....

# COMMAND ----------

print(paste(sum(is.na(products$discounted_price)), sum(is.na(products$actual_price))))
#por algum motivo alguns preços estão sendo perdidos na DF PRODUCTS.

# COMMAND ----------

#removendo esses valores da FATO.
product_id_na <- products[is.na(products$discounted_price) | is.na(products$actual_price),]$product_id
fato <- fato[!fato$product_id %in% product_id_na,]

# COMMAND ----------

head(fato)


# COMMAND ----------

head(users)


# COMMAND ----------

head(reviews)

# COMMAND ----------

head(products)

# COMMAND ----------

# MAGIC %md
# MAGIC WRITING SCHEMA

# COMMAND ----------

fato.spark <- as.DataFrame(fato)
users.spark <- as.DataFrame(users)
reviews.spark <- as.DataFrame(reviews)
products.spark <- as.DataFrame(products)


# COMMAND ----------

write.parquet(fato.spark, "amazon_star_schema/fato.parquet")
write.parquet(users.spark, "amazon_star_schema/users.parquet")
write.parquet(reviews.spark, "amazon_star_schema/reviews.parquet")
write.parquet(products.spark, "amazon_star_schema/products.parquet")
# onde eles foram parar?

# COMMAND ----------

fato <- read.df("/fato.parquet", "parquet")

# COMMAND ----------

head(fato)
# aparentemente está tudo ok