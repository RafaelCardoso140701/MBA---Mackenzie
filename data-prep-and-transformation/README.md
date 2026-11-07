# Data Prep & Transformation

Disciplina do MBA em Engenharia de Dados (Mackenzie) — Prof. Luciano Gillieron Gavinho, PhD.

## Aula 4 · Limpeza e padronização: do bronze ao silver

**Notebook:** [`Aula04_Pratica_Limpeza_Silver_Olist.ipynb`](Aula04_Pratica_Limpeza_Silver_Olist.ipynb)

Construção da camada **silver** a partir dos CSVs públicos da Olist (camada bronze), usando DuckDB no Google Colab. Cada regra de limpeza aplicada fica registrada num log, com o número de linhas afetadas, e cada resultado é validado por testes de qualidade que devem retornar zero violações.

**Stack:** Python · DuckDB · pandas · Google Colab

### Princípios seguidos

- **Medir antes de limpar:** perfil de cada tabela com `SUMMARIZE` antes de qualquer alteração.
- **Nenhuma linha válida some:** problemas são **marcados** com flags; só duplicatas comprovadas são removidas.
- **Toda regra é rastreável:** o log final tem 20 regras, e as remoções somadas batem exatamente com a diferença entre bronze e silver.
- **Todo resultado é testado:** 14 testes de qualidade, todos aprovados.

### Principais achados

| Problema | Impacto | Decisão |
|---|---|---|
| CEP lido como número, perdendo o zero à esquerda | 23.995 clientes (24%) e 1.027 vendedores (33%) afetados; o JOIN com geolocalização caía de 99,7% para 75,6% de correspondência | Corrigido com `lpad` para texto de 5 dígitos |
| Cidade de vendedores em texto livre (`sao paulo - sp`, `sbc/sp`, CEP ou e-mail no campo) | 611 grafias para 587 cidades; 35 cidades só existem em outra UF e 39 não foram encontradas | Padronizada; casos sem correspondência marcados com `flag_cidade` |
| `review_id` repetido em 1.603 linhas | O conteúdo era idêntico: a mesma avaliação cobre vários pedidos. Além disso, 547 pedidos têm mais de uma avaliação (202 com notas diferentes) | Chave da silver = `(review_id, order_id)`; relação N:N documentada; nenhuma linha removida |
| `geolocation` redundante | 279.667 linhas removidas (28%): 261.831 duplicatas exatas e 17.836 que só diferiam na grafia da cidade | Removidas; 33 coordenadas fora do Brasil marcadas |
| Datas incoerentes em pedidos | 1.382 pedidos com coleta antes da compra/aprovação ou entrega antes da coleta | Marcados com `flag_datas_incoerentes`, sem imputar datas |
| Preço com cauda longa (média R$ 120,65 × mediana R$ 74,99) | O IQR no preço bruto marcaria 7,5% dos itens; no log do preço, 1,1% | Não removidos: são produtos caros reais |

### Tabelas silver geradas

| Tabela | Bronze | Silver | Observação |
|---|---:|---:|---|
| `silver_customers` | 99.441 | 99.441 | CEP com 5 dígitos, cidade padronizada |
| `silver_sellers` | 3.095 | 3.095 | CEP com 5 dígitos, cidade padronizada, `flag_cidade` |
| `silver_geolocation` | 1.000.163 | 720.496 | Sem duplicatas, `flag_fora_do_brasil` |
| `silver_orders` | 99.441 | 99.441 | Flags de entrega sem data, cancelado com data e datas incoerentes |
| `silver_products` | 32.951 | 32.951 | Peso 0 convertido em nulo |
| `silver_items` | 112.650 | 112.650 | `flag_frete_zero` |
| `silver_reviews` | 99.224 | 99.224 | Chave `(review_id, order_id)` testada |

### Decisões que dependem do negócio

- Qual avaliação vale para o pedido quando há mais de uma com notas diferentes (primeira, última ou média)?
- Nos 1.359 pedidos coletados antes da aprovação: a aprovação é gravada com atraso ou a "coleta" registra a emissão da etiqueta?
