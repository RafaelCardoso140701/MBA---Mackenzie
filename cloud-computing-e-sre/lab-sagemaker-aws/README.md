# Laboratório AWS — Engenharia de Dados, ML e IA generativa

## Contexto

Atividade prática baseada no workshop **Next Generation of Amazon SageMaker**. No cenário fictício da **AnyCompany Food & Beverage**, o objetivo é usar dados de vendas e avaliações de produtos para apoiar decisões de marketing e produzir uma campanha personalizada.

## Tecnologias utilizadas

Amazon SageMaker Unified Studio · AWS Glue Data Catalog · Amazon Athena · Amazon Redshift · PySpark · Amazon Bedrock · JupyterLab · Python · Pandas · Scikit-learn · XGBoost · HTML/CSS

## Etapas do laboratório

| Etapa | Implementação / evidência |
|---|---|
| 1. Exploração de dados | [SQL — lucro por região e país em 2024](sql/analise_lucro_2024.sql) no catálogo de dados da AWS |
| 2. Integração Lakehouse | [Notebook de integração](notebooks/join_lakehouse_tables.ipynb) com dados de vendas e promoções via PySpark |
| 3. Publicação e compartilhamento | Data Product **Product Sales and Reviews**, com `product_reviews` e `sales_table_enriched_w_campaigns`; assinatura do `project_ml` confirmada como **Shared** na interface |
| 4. Machine Learning | [Notebook de previsão](notebooks/ml-model-development.ipynb) e [utilitários Python](scripts/lab_utilities.py) com preparação dos dados, XGBoost e geração de prompts |
| 5. Campanha | [Página HTML](campaign/campaign.html) de **Organic Breakfast Cereals**, com conteúdo segmentado para profissionais com rotina corrida |

## Estrutura do projeto

```text
lab-sagemaker-aws/
├── README.md
├── sql/
│   └── analise_lucro_2024.sql
├── notebooks/
│   ├── join_lakehouse_tables.ipynb
│   └── ml-model-development.ipynb
├── scripts/
│   └── lab_utilities.py
├── campaign/
│   └── campaign.html
└── evidencias/
    └── README.md
```

**Arquivos complementares ainda não publicados neste repositório:** `campaign/generated_image.png`, `campaign/generated_image (1).png` e `evidencias/Entrega_LAB_MBA.pdf`. O PDF foi produzido a partir da página da campanha e reúne três páginas.

## Executando os notebooks

Os notebooks foram criados para o ambiente **SageMaker Unified Studio** e exigem projetos, permissões, conexões e recursos de computação AWS adequados. Não são notebooks locais autossuficientes.

1. Execute o notebook de integração no projeto `project_analytics` para preparar `sales_table_enriched_w_campaigns`. Ele utiliza a magic `%%pyspark project.spark.fineGrained`, específica do ambiente.
2. Publique o Data Product e disponibilize seus dois ativos para o `project_ml`.
3. Execute o notebook de previsão no `project_ml`. Ele importa `lab_utilities`: no JupyterLab, coloque `lab_utilities.py` ao lado do notebook **ou** inclua o diretório `scripts/` no `sys.path` antes da importação.
4. Para visualizar a campanha, coloque os dois arquivos PNG na mesma pasta de `campaign.html` com os nomes mencionados acima e abra a página em um navegador.

As consultas SQL não incluem resultados numéricos, porque as saídas da execução não foram anexadas. Os notebooks também não incluem outputs salvos: **a presença do código não comprova execução ou desempenho do treinamento**.

## Notas técnicas e limitações

- O notebook de integração usa `CREATE TABLE`; uma nova execução pode exigir tratamento para uma tabela já existente.
- O script de ML prevê `units_sold` usando atributos que incluem receita, custo e lucro. É necessário avaliar possível **vazamento de informação (data leakage)** e garantir disponibilidade das variáveis no momento da previsão.
- A consulta via Athena precisa ser revista para grandes volumes, especialmente quanto à paginação dos resultados.
- O conteúdo da campanha e seus depoimentos são **ilustrativos**, relativos a um caso fictício.
- O tutorial solicitava **Amazon Nova Canvas**, que não estava disponível no ambiente utilizado. As imagens da campanha foram geradas **fora do Amazon Bedrock**; portanto, não são evidências de geração pelo Nova Canvas na AWS.

## Referência

[Workshop — Next Generation of Amazon SageMaker](https://catalog.us-east-1.prod.workshops.aws/event/dashboard/en-US/workshop/1-introduction/use-case-intro)

> **Aviso de segurança:** não publique chaves de acesso, tokens, credenciais, ARNs sensíveis ou prints contendo identificadores da conta.
