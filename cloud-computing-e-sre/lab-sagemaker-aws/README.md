# Laboratório AWS — SageMaker Unified Studio e Amazon Bedrock

## Visão geral

Laboratório da empresa fictícia **AnyCompany Food & Beverage** para analisar vendas, combinar dados do Lakehouse com promoções, compartilhar ativos entre projetos e preparar campanhas personalizadas.

## Tecnologias

SageMaker Unified Studio · AWS Glue Data Catalog · Amazon Athena · Amazon Redshift · PySpark · Amazon Bedrock · Python · Pandas · Scikit-learn · XGBoost · JupyterLab · HTML/CSS

## Etapas

1. **Exploração SQL:** consultas de lucro por região e país em 2024, disponíveis em [sql/analise_lucro_2024.sql](sql/analise_lucro_2024.sql).
2. **Integração de dados:** notebook `join_lakehouse_tables.ipynb` combina `customer_insights_db.sales_data` (Glue Data Catalog) com `rms_federated_catalog.public.promotions` (Redshift federado), considerando região, categoria e período da promoção. O resultado é `sales_table_enriched_w_campaigns`.
3. **Catálogo:** publicação de **Product Sales and Reviews**, reunindo `product_reviews` e `sales_table_enriched_w_campaigns`. Assinatura aprovada e compartilhada com o `project_ml` (**Shared**).
4. **Machine Learning:** notebook `ml-model-development.ipynb` consulta dados no Athena e utiliza `lab_utilities.py` para preparação, treino de XGBoost, projeções de vendas e geração de prompts de marketing.
5. **IA generativa e campanha:** página `campaign.html` sobre Organic Breakfast Cereals, com conteúdo, duas imagens e exportação em PDF.

## Estrutura da entrega completa

```text
lab-sagemaker-aws/
├── README.md
├── sql/analise_lucro_2024.sql
├── notebooks/
│   ├── join_lakehouse_tables.ipynb
│   └── ml-model-development.ipynb
├── scripts/lab_utilities.py
├── campaign/
│   ├── campaign.html
│   ├── generated_image.png
│   └── generated_image (1).png
└── evidencias/Entrega_LAB_MBA.pdf
```

**Observação:** os notebooks, o utilitário Python, o HTML, as imagens e o PDF são complementos da entrega; os arquivos binários e os notebooks podem exigir envio separado ao GitHub. Consulte a pasta no repositório para saber o que já está publicado.

## Observações técnicas

- Os notebooks dependem de credenciais e recursos autorizados na AWS e não foram fornecidos com resultados de execução salvos.
- O notebook de integração usa `CREATE TABLE`, que pode falhar se a tabela de saída já existir.
- A preparação para ML inclui variáveis como receita, custo e lucro, que podem causar vazamento de informação ao prever `units_sold`; esse ponto requer validação.
- A leitura via Athena pode precisar de paginação para tabelas maiores.
- As imagens foram criadas **fora do Amazon Bedrock**, pois o Nova Canvas não estava disponível no ambiente do laboratório; não são resultados de execução do modelo no SageMaker.

## Referência

[Workshop AWS — Next Generation of Amazon SageMaker](https://catalog.us-east-1.prod.workshops.aws/event/dashboard/en-US/workshop/1-introduction/use-case-intro)

> Não publicar credenciais, tokens ou imagens contendo informações sensíveis da conta AWS.
