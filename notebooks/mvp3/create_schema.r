# Databricks notebook source
# MAGIC %md
# MAGIC # Etapa 2 - Criação do Schema estrela:
# MAGIC Para a criação do Schema estrela, o desafio será usar  `R` e o `SparkR`. Como os dados originais estão salvos em *parquet*, a primeira leitura precisa ser realizada com o `SparkR` e depois transformada para o `R` para a criação do *schema*

# COMMAND ----------

# MAGIC %md
# MAGIC ### Leitura e separação inicial da base:

# COMMAND ----------

#bibliotecas 
library(dplyr)
library(SparkR)

# COMMAND ----------

# Leitura do parquet amazon
amazon <- read.df("/FileStore/tables/amazon.csv", source = "csv", header="true", inferSchema = "true")
amazon <- drop(amazon, c("review_content","about_product")) #necessario para trazer o DF para DF em R e não em SparkR
head(amazon)

# COMMAND ----------

# MAGIC %md
# MAGIC Obstáculo: A transformação do DF de sparkR para R, não contempla celulas grandes. Portanto, foi necessário retirar as colunas "review_content" e "about_product" para fazer essa transformação.

# COMMAND ----------

# O DF amazon realmente está em SparkDF em RSpark
class(amazon)

# COMMAND ----------

# MAGIC %md
# MAGIC Analisando alguns *product_id* da base (como o da célula a seguir), percebi um problema na transferência de `SparkR` para `R`: algumas linhas se misturam na propria leitura do DF, ficam "deslocadas" após o *product_name*. neste caso, vemos que o product name "entra dentro" das categorias. e fica tudo bagunçado. Portanto, além da perda das duas colunas *review_content* e *about_product*, teremos perdas em algumas linhas (alguns produtos) em que isso acontece.

# COMMAND ----------

head(amazon[amazon$product_id=='B0BNVBJW2S',])

# COMMAND ----------

# transformação do DF Spark para R
amaz <- as.data.frame(amazon)
class(amaz)

# COMMAND ----------

# MAGIC %md
# MAGIC Agora que o DF está no formato do R, podemos começar com o processo de construção do schema. Como visto na leitura inicial dos dados, as colunas *user_id,user_name,review_id e review_title* possuem 8 elementos em cada. Ou seja, cada produto tem 8 reviews escritos por 8 usuários diferentes. Aqui, o objetivo é criar essas bases separadas, para usuários, produtos e reviews.

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

df_users <- df_users %>% dplyr::distinct()

# COMMAND ----------

# MAGIC %md
# MAGIC obstáculo: Para a separação do *review_title*, é bem difícil definir aonde fica a separação dos textos. Alguns separam facilmente por virgulas. Porém, quando algum título de produto único possui virgulas, essa separação não fica clara. O comando em REGEX, auxilia nessa separação e cria algumas exceções. O comando tryCatch vai executar o looping e pular, quando ouver o erro. Portanto, aqui existe mais perda de informação devido a esta extração.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Criando as Tabelas FATO, PRODUCTS, USERS e REVIEWS

# COMMAND ----------

# MAGIC %md
# MAGIC Observando alguns displays, foi possível observar que alguns produtos, com product_id diferentes, possuem os mesmos reviews e usuários. (Uma comparação de exemplo pode ser vista nas imagens 2 e 3 no readme)

# COMMAND ----------

# MAGIC %md
# MAGIC Neste caso, vamos criar um ranking dessas observações e manter apenas uma delas. de forma aleatória.

# COMMAND ----------

# checando os produtos iguais, mas product_id diferentes. Neste caso, o review_id e o user_id são os mesmos, porem o product_id é diferente.
prod_diff <- df_users %>% 
dplyr::group_by(user_id,user_name,review_id) %>%
dplyr::mutate(id_order = rank(review_id,ties.method= "random"))


# COMMAND ----------

#podemos ver claramente que o produto é essencialmente o mesmo.
display(prod_diff[prod_diff$review_id=="RGIQEG07R9HS2",])

# COMMAND ----------

prod_diff2 <- prod_diff %>% dplyr::filter(id_order==1) %>% dplyr::select(-c(id_order))
# confirmando se temos agora produtos unicos com usuarios e reviews unicos:
n_before <- nrow(prod_diff2)
n_after <- nrow(prod_diff2 %>% dplyr::group_by(user_id,review_id,product_id) %>% dplyr::count())

df_users2 <- prod_diff2
print(paste(n_before,n_after)) # existe ainda  uma pequena duplicação de 11 observações. Mas está suficientemente aceitavel

# COMMAND ----------

# criando as tabelas do schema
fato = df_users2[,c('user_id','review_id','product_id')]
users = df_users2[,c('user_id','user_name')]
reviews = df_users2[,c('review_id','review_title')]
products0 = amaz[,(names(amaz) %in% c("product_id", "product_name", "discounted_price", "actual_price","discount_percentage", "rating","rating_count"))]



categories <- df_users2 %>% dplyr::group_by(product_id,category) %>% dplyr::count() %>% dplyr::select(-c(n))

products <- merge(products0,categories,by="product_id")

products <- products %>% dplyr::filter(product_id %in% df_users2$product_id)


fato <- dplyr::distinct(fato)
users <- dplyr::distinct(users)
reviews <- dplyr::distinct(reviews)
products <- dplyr::distinct(products)


products$actual_price <- gsub('₹','',products$actual_price)
products$discounted_price <- gsub('₹','',products$discounted_price)
products$rating_count <- as.numeric(gsub(',','',products$rating_count))

#Rupias Indianas Para Reais: (valor em 01/07/2024), imagem no github
products$actual_price <-round(as.numeric(gsub(",","",products$actual_price))*0.067,2)
products$discounted_price <-round(as.numeric(gsub(",","",products$discounted_price))*0.067,2)


# COMMAND ----------

# MAGIC %md
# MAGIC Aqui, por conta de alguma "bagunça" que ocorre na transformação do *CSV* em *PARQUET*, algumas linhas de produtos perderam os seus valores, por isso o Warning acima ocorreu. A decisão é de seguir com a base desta mesma forma, com mais esta perda de informação. No comando à seguir é possível ver um exemplo de como ficou esta bagunça, na leitura do parquet original 'amazon'

# COMMAND ----------

display(amazon[amazon$product_id=='B0BNVBJW2S',])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Resultado das bases do schema estrela:

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
# MAGIC ### Escrevendo as bases em parquet no databricks

# COMMAND ----------

# transformando de R para RSpark
fato.spark <- as.DataFrame(fato)
users.spark <- as.DataFrame(users)
reviews.spark <- as.DataFrame(reviews)
products.spark <- as.DataFrame(products)


# COMMAND ----------

write.parquet(fato.spark, "amazon_star_schema/fato.parquet","overwrite")
write.parquet(users.spark, "amazon_star_schema/users.parquet","overwrite")
write.parquet(reviews.spark, "amazon_star_schema/reviews.parquet","overwrite")
write.parquet(products.spark, "amazon_star_schema/products.parquet","overwrite")
#

# COMMAND ----------

# MAGIC %md
# MAGIC Testando a leitura:

# COMMAND ----------

fato <- read.df("/fato.parquet", "parquet")

# COMMAND ----------

head(fato)
# aparentemente está tudo ok