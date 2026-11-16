# Laboratório AWS — SageMaker Unified Studio e Amazon Bedrock

## Contexto

Workshop da empresa fictícia **AnyCompany Food & Beverage**, que utiliza engenharia de dados e inteligência artificial para apoiar uma campanha de marketing personalizada em um cenário de redução nas vendas e no orçamento de marketing.

## Tecnologias

Amazon SageMaker Unified Studio · AWS Glue Data Catalog · Amazon Athena (SQL) · Amazon Bedrock · JupyterLab · HTML/CSS

## Fluxo do laboratório

1. **Descoberta e análise:** acesso aos dados de vendas em `AwsDataCatalog.customer_insights_db.sales_data` e consultas de lucro por região e país para 2024, disponíveis em [sql/analise_lucro_2024.sql](sql/analise_lucro_2024.sql).
2. **Engenharia de dados:** preparação do projeto `project_analytics`, com a tabela `sales_table_enriched_w_campaigns`.
3. **Data Product:** criação de **Product Sales and Reviews**, reunindo `product_reviews` e `sales_table_enriched_w_campaigns`.
4. **Governança e compartilhamento:** assinatura pelo `project_ml`, confirmada com status **Approved / Shared**.
5. **IA generativa e marketing:** elaboração de campanha para **Organic Breakfast Cereals** e criação de uma página HTML.
6. **Validação:** abertura da página `campaign.html` no JupyterLab e exportação de PDF com três páginas.

## Estrutura

```text
lab-sagemaker-aws/
├── README.md
├── sql/
│   └── analise_lucro_2024.sql
├── campaign/
│   ├── campaign.html
│   ├── generated_image.png
│   └── generated_image (1).png
└── evidencias/
    └── Entrega_LAB_MBA.pdf
```

Os arquivos de imagem e o PDF poderão ser adicionados na sequência. **O HTML depende das duas imagens na mesma pasta**.

## Campanha HTML

O arquivo [campaign/campaign.html](campaign/campaign.html) contém conteúdo para profissionais com rotina corrida: mensagens para redes sociais, benefícios, casos de uso e depoimentos ilustrativos. Para visualizar localmente, baixe a pasta `campaign` completa e abra `campaign.html` no navegador.

### Adaptação documentada

O tutorial do workshop utilizava **Amazon Nova Canvas** para gerar imagens. Esse modelo não estava disponível no ambiente acessado. Por isso, **as imagens finais foram geradas fora do Amazon Bedrock**, e não devem ser atribuídas ao Nova Canvas. O restante da página foi montado e visualizado no JupyterLab.

### Escopo e limitações

- As consultas SQL estão preservadas como scripts; este repositório não atribui resultados numéricos sem a saída da execução.
- O status **Shared** foi verificado na interface do SageMaker.
- O HTML e a exportação em PDF foram verificados.
- Não há evidência incluída aqui de treinamento de um modelo preditivo completo.
- Depoimentos e marca do estudo de caso são fictícios.

## Referência

[Workshop — Next Generation of Amazon SageMaker](https://catalog.us-east-1.prod.workshops.aws/event/dashboard/en-US/workshop/1-introduction/use-case-intro)

> Não publicar credenciais, tokens, URLs assinadas nem capturas de tela com identificadores sensíveis da AWS.
