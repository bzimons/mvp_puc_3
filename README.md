# MVP de Engenharia de Dados
Aluna : Beatriz Leal Simoes e Silva

## Objetivo:
Construção de um MVP satisfatório para a disciplina, considerando o desafio abordar a engenharia de dados em suas etapas com grande foco e por fim passar pela etapa de análise de dados.
Perguntas e Desafios a se responder:
- Utilizar o `SparkR` e  `R` no `Databricks` para realizar o ETL.
- Construir um schema estrela com a base escolhida.
- Aproveitar os dados em array conjuntos em sua totalidade.
- Perguntas da análise:
  - No processo de limpeza, houve perda de informação?
  - Como ficou a criação do catalogo de dados e da representação do Schema final?


## Etapa 1 : 
Escolha e carregamento dos dados. O conjunto de dados escolhido foi de produtos e reviews da [amazon, encontrado na plataforma _kaggle_](https://www.kaggle.com/datasets/karkavelrajaj/amazon-sales-dataset?resource=download). O dataset é composto de produtos, suas respectivas páginas de compras e uma amostra de 8 reviews de usuários para cada produto.
A partir deste conjunto inicial, é elaborado uma etapa simples de ETL, como parte do desafio em lidar com a engenharia de dados.
O arquivo CSV foi carregado na parte de Create New Table do  `DataBricksCommunityEdition`, a imagem da página em que ele é carregado pode ser vista [aqui](https://github.com/bzimons/mvp_puc_3/blob/databricks/imagens/carga_databricks.PNG).

A partir disso, o carregamento do arquivo para dentro do ambiente em formato *parquet* é feita no notebook [create_amazon]
(https://github.com/bzimons/mvp_puc_3/blob/main/notebooks/mvp3/create_amazon.py) E o catálago de dados pode ser visto em [catálogo](https://github.com/bzimons/mvp_puc_3/blob/main/catalogo_dados.md).

TIRAR PRINT DA ETAPA DO CSV JOGADO NO DATABRICKS

## Etapa 2 : 
Etapa de limpeza, transformação de dados e criação do SCHEMA para que os dados possam ser analizados. O notebook desta etapa é o [create_schema](https://github.com/bzimons/mvp_puc_3/blob/main/notebooks/mvp3/create_schema.r). 

Observação: Nesta etapa, é notável que alguns produtos com *product_id* diferentes são essencialmente os mesmos produtos. O print de um exemplo desses produtos pode ser visto aqui na comparação [imagem 1](https://github.com/bzimons/mvp_puc_3/blob/main/imagens/amazon1.png), [imagem 2](https://github.com/bzimons/mvp_puc_3/blob/main/imagens/amazon2.png) e [imagem 3](https://github.com/bzimons/mvp_puc_3/blob/main/imagens/amazon3.png).

### Resultado final do schema:

![star_schema](https://github.com/bzimons/mvp_puc_3/blob/databricks/imagens/star_schema.PNG)

## Etapa 3 : 

A partir do schema, as perguntas dos objetivos podem ser respondidas. O notebook de análises é [data_analysis](https://github.com/bzimons/mvp_puc_3/blob/main/notebooks/mvp3/data_analysis.r)

## Auto avaliação : 

O desafio de criar um SCHEMA e trabalhar com o Databricks foi efetivo. Porém na construção do SCHEMA, muitas informações  foram perdidas devido ao carregamento no databricks e na transformação para o data.frame no R. Dito isto, o processo do SCHEMA poderia ter sido feito no PySpark, exigindo aqui um pouco mais de dedicação em aprender a linguagem.

As perguntas na parte final de análise, foram devidamente respondidas dentro do SCHEMA criado. Se algumas variáveis da base original tivessem sido retidas, como a descrição das reviews por completo, talvez fosse possível


