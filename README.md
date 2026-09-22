# MBA em Engenharia de Dados — Mackenzie

Trabalhos e projetos das disciplinas do MBA em Engenharia de Dados da
Universidade Presbiteriana Mackenzie.

**Rafael Cardoso Nascimento**

## Disciplinas

| Pasta | Disciplina | Conteúdo |
|---|---|---|
| [`arquitetura-de-dados/`](arquitetura-de-dados/) | Arquitetura de Dados | Data Mesh, Mercado Inteligente, Governança e Segurança, estratégia de dados para supermercados |
| [`big-data-processing/`](big-data-processing/) | Big Data Processing | **Projeto final:** pipeline Medallion com PySpark, Airflow e Docker |
| [`dados-e-analytics-organizacoes/`](dados-e-analytics-organizacoes/) | Dados e Analytics nas Organizações | Matriz TOWS e trabalho final |
| [`data-science-experience/`](data-science-experience/) | Data Science Experience | Análise exploratória de gêneros musicais, EDA com pandas-profiling |
| [`data-visualization/`](data-visualization/) | Data Visualization | Dashboards em Tableau e trabalho final sobre a base SARESP |
| [`desenvolvimento-profissional/`](desenvolvimento-profissional/) | Desenvolvimento Profissional | PDI e mentoring |
| [`linguagens-de-programacao/`](linguagens-de-programacao/) | Linguagens de Programação para Dados e Analytics | Análise de cancelamentos, lead time de entrega, segmentação RFM, meios de pagamento, EDA Olist |

## Destaque

**[Projeto Final — Big Data Processing](big-data-processing/projeto-final/)**

Pipeline de produção containerizado que processa 1,5 milhão de registros de
vendas de e-commerce: ingestão multi-formato, arquitetura Medallion, seis
validações de qualidade com quarentena funcional e orquestração via Apache
Airflow. Sobe inteiro com um `docker compose up`.

PySpark 3.5 · Airflow 2.8 · Docker Compose · Parquet
