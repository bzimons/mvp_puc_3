# Catálogo de dados para cada arquivo

## Dataset: products.parquet

| Coluna | Descrição |
|:---|:---|
|product_id | ID única do produto |
|product_name | nome do produto |
|discounted_price | preço com valor em desconto |
|actual_price |preço real |
|discount_percentage | porcentagem do preço em desconto, de 0% à 100%|
|rating |nota média do produto, de 1.0 à 5.0 |
|category |categoria principal em que o produto se encontra |


## Dataset: users.parquet

| Coluna | Descrição |
|:---|:---|
|user_id | ID única do usuário|
|user_name | nome de display do usuário |

## Dataset: reviews.parquet

| Coluna | Descrição |
|:---|:---|
|review_id | ID única da review |
|review_title | título da review feita pelo usuário|


## Dataset: fato.parquet

| Coluna | Descrição |
|:---|:---|
|product_id | ID única do produto |
|review_id | ID única da review |
|user_id | ID única do usuário|
