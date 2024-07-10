# MVP de Engenharia de Dados
Aluna : Beatriz Leal Simoes e Silva

## Objetivo:
Construção de um MVP satisfatório para a disciplina, considerando o desafio abordar a engenharia de dados em suas etapas com grande foco e por fim passar pela etapa de análise de dados.
Perguntas e Desafios a se responder:
- Utilizar o `SparkR` e  `R` no `Databricks` para realizar o ETL.
- É possível construir um schema estrela com a base escolhida?
- Os dados no processo de ETL, foram aproveitados com totalidade?
- No processo de limpeza, houve perda de informação?
- Como ficou a criação do catalogo de dados e da representação do Schema final?


## Etapa 1 : 
Escolha e carregamento dos dados. O conjunto de dados escolhido foi de produtos e reviews da [amazon, encontrado na plataforma _kaggle_](https://www.kaggle.com/datasets/karkavelrajaj/amazon-sales-dataset?resource=download). O dataset é composto de produtos, suas respectivas páginas de compras e uma amostra de 8 reviews de usuários para cada produto.
A partir deste conjunto inicial, é elaborado uma etapa simples de ETL, como parte do desafio em lidar com a engenharia de dados.
O arquivo CSV foi carregado na parte de `catalog/databases` do  `DataBricksCommunityEdition`. A partir disso, o carregamento do arquivo para dentro do ambiente em formato *parquet* é feita no notebook [create_amazon](https://github.com/bzimons/mvp_puc_3/blob/main/notebooks/mvp3/create_amazon.py) E o catálago de dados pode ser visto em [catálogo](https://github.com/bzimons/mvp_puc_3/blob/main/catalogo_dados.md).


## Etapa 2 : 
Etapa de limpeza e correção dos dados. Aqui o  foco é transformar cada categoria em seu tipo correto e realizar transformações e limpeza para que os dados possam ser analizados diretamente, sem passar pro processos de limpeza. O notebook desta etapa é XXX

![star_schema](https://github.com/bzimons/mvp_puc_3/blob/databricks/imagens/star_schema.PNG)

## Etapa 3 : 

A partir das bases refinadas, as seguintes perguntas de análise são definidas:
Qual a

Detalhamento
Busca pelos dados
O conjunto de dados hearts foi, inicialmente, obtido no kaggle, uma fonte reconhecida por disponibilizar conjuntos de dados de alta qualidade para projetos de machine learning, data science e data engineering. Isso garante a confiabilidade e a integridade dos dados utilizados nesta sprint.

