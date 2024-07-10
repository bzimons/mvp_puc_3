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
O arquivo CSV foi carregado na parte de `catalog/databases` do  `DataBricksCommunityEdition`. A partir disso, o carregamento do arquivo para dentro do ambiente em formato *parquet* é feita no notebook [create_amazon](https://github.com/bzimons/mvp_puc_3/blob/main/notebooks/mvp3/create_amazon.py) E o catálago de dados pode ser visto em [catálogo](https://github.com/bzimons/mvp_puc_3/blob/main/catalogo_dados.md).


## Etapa 2 : 
Etapa de limpeza, transformação de dados e criação do SCHEMA para que os dados possam ser analizados. [create_schema](https://github.com/bzimons/mvp_puc_3/blob/main/notebooks/mvp3/create_schema.py). Nesta etapa, é notável que alguns produtos com *product_id* diferentes são essencialmente os mesmos produtos. O print de um exemplo desses produtos pode ser visto aqui na comparação [imagem 1](https://github.com/bzimons/mvp_puc_3/blob/main/imagens/amazon1.png), [imagem 2](https://github.com/bzimons/mvp_puc_3/blob/main/imagens/amazon2.png) e [imagem 3](https://github.com/bzimons/mvp_puc_3/blob/main/imagens/amazon3.png).

### Resultado final do schema:

![star_schema](https://github.com/bzimons/mvp_puc_3/blob/databricks/imagens/star_schema.PNG)

## Etapa 3 : 

A partir das bases refinadas, as seguintes perguntas de análise são definidas:
Qual a

Detalhamento
Busca pelos dados
O conjunto de dados hearts foi, inicialmente, obtido no kaggle, uma fonte reconhecida por disponibilizar conjuntos de dados de alta qualidade para projetos de machine learning, data science e data engineering. Isso garante a confiabilidade e a integridade dos dados utilizados nesta sprint.

## Auto avaliação : 
