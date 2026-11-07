# Data Prep & Transformation

Disciplina do MBA em Engenharia de Dados (Mackenzie) — Prof. Luciano Gillieron Gavinho, PhD.

| Aula | Tema | Notebook |
|---|---|---|
| 1 | Diagnóstico das fontes transacionais: inventário, grão, chaves, tipos e integridade | [`Aula01_Pratica_Diagnostico_Olist.ipynb`](Aula01_Pratica_Diagnostico_Olist.ipynb) |
| 2 | Engenharia reversa do modelo transacional, formas normais, restrições e modelagem em documento | [`Aula02_Pratica_Modelagem_Olist.ipynb`](Aula02_Pratica_Modelagem_Olist.ipynb) |
| 4 | Limpeza e padronização: do bronze ao silver | [`Aula04_Pratica_Limpeza_Silver_Olist.ipynb`](Aula04_Pratica_Limpeza_Silver_Olist.ipynb) |

## Aula 1 · Diagnóstico das fontes transacionais da Olist

**Notebook:** [`Aula01_Pratica_Diagnostico_Olist.ipynb`](Aula01_Pratica_Diagnostico_Olist.ipynb)

Primeiro contato com os CSVs da Olist, extraídos do ambiente transacional (OLTP) sem tratamento. Antes de construir qualquer pipeline: o que existe, qual o grão de cada tabela e o que quebra se o dado for usado direto.

**Stack:** Python · DuckDB · pandas · Google Colab

### Principais achados

| Achado | Evidência |
|---|---|
| Linhas do arquivo ≠ registros | `reviews` tem 104.719 linhas físicas para 99.224 registros. A diferença é **exatamente** o número de quebras de linha (`\r\n`) dentro de 3.852 comentários: `wc -l` não serve como controle de carga para CSV com texto livre |
| `customer_id` não é o cliente | 99.441 IDs para 96.096 pessoas; 2.997 pessoas com mais de um ID |
| `review_id` não é chave | 98.410 distintos em 99.224 linhas; 547 pedidos com mais de uma avaliação |
| CEP perde o zero à esquerda | Inferido como BIGINT em clientes e vendedores (sem aspas no CSV) e VARCHAR em geolocation (entre aspas); 23.995 clientes com menos de 5 dígitos |
| Geolocalização redundante | 261.831 duplicatas exatas; 1.000.163 linhas para 19.015 prefixos |
| Pedidos incompletos | 775 sem itens, 1 sem pagamento, 768 sem avaliação |
| Incoerências temporais | 61 pedidos entregues antes de aprovados; 166 coletados antes da compra |
| Grafia e codificação | `são paulo`, `sãopaulo` e `sa£o paulo` convivem com `sao paulo` |
| Catálogo | 610 produtos sem categoria; 2 categorias sem tradução; colunas com erro de digitação (`lenght`) |
| Valores que não fecham | 312 pedidos com diferença acima de R$ 1,00 entre itens e pagamentos |

O notebook termina com o **mapa de uso** (quais achados atrapalham cinco perguntas de negócio) e a **Tabela de Achados**, com 13 problemas, evidência, impacto e tratamento provável. Ela é o ponto de partida das Aulas 2 e 4.

## Aula 2 · Lendo e reconstruindo o modelo transacional da Olist

**Notebook:** [`Aula02_Pratica_Modelagem_Olist.ipynb`](Aula02_Pratica_Modelagem_Olist.ipynb)

Engenharia reversa do modelo da Olist a partir dos próprios dados: medição das cardinalidades reais, testes de dependência funcional contra a 2FN e a 3FN, imposição de PKs e FKs que os CSVs não têm e remodelagem do pedido como documento JSON, no estilo de um banco NoSQL.

**Stack:** Python · DuckDB · pandas · Google Colab

### Cardinalidades medidas

| Relacionamento | Mín | Máx | Cardinalidade | Leitura |
|---|---:|---:|---|---|
| `customer_id` → pedidos | 1 | 1 | 1:1 | `customer_id` é o cliente **num pedido**, não a pessoa |
| pessoa (`customer_unique_id`) → pedidos | 1 | 17 | 1:N | 2.997 pessoas compraram mais de uma vez |
| pedido → itens | 0 | 21 | 1:N | 775 pedidos sem nenhum item |
| pedido → pagamentos | 0 | 29 | 1:N | 2.961 pedidos com mais de um meio de pagamento |
| pedido ↔ avaliações | 0 | 3 | N:N | 547 pedidos com várias avaliações; 789 avaliações em vários pedidos |

Nenhum item aponta para produto, vendedor ou pedido inexistente. Por outro lado, **todos** os produtos foram vendidos e **todos** os vendedores venderam: `products` e `sellers` foram extraídos a partir das vendas e não são os cadastros completos (viés de seleção).

### Principais achados

| Teste | Resultado | Conclusão |
|---|---|---|
| Grão de `items` | 7.088 pares pedido-produto repetidos; um pedido com 20 linhas idênticas | Uma linha = **uma unidade** vendida; a quantidade é `COUNT(*)` |
| Frete por unidade? | Em 99,5% dos pedidos, `SUM(price) + SUM(freight_value)` bate ao centavo com o total pago | `freight_value` é **por unidade**, assim como `price` |
| `seller_zip_code_prefix → seller_state` (3FN) | 17 prefixos com mais de um estado; quase sempre a cidade correta com estado SP | Dependência transitiva; hipótese de SP como valor padrão no cadastro do vendedor. Em clientes, 0 violações |
| `product_id → price` (2FN) | 39,8% dos produtos revendidos têm mais de um preço; até 207% de variação | O preço é um fato **da venda**: a modelagem atual está correta |
| Anomalia de atualização | Com o status repetido em cada item, atualizar uma linha deixou 9.762 pedidos com dois status | `order_id → order_status` é dependência parcial: viola a 2FN |
| PK em `review_id` | Banco recusou a carga inteira (0 linhas) | Chave correta: `(review_id, order_id)`, com 99.224 de 99.224 linhas carregadas |
| PK em `geolocation` pelo prefixo | 52,6 linhas por prefixo; 8 prefixos com mais de um estado | Proposta `t_cep`: uma linha por prefixo, coordenada mediana e cidade/UF mais frequentes |

### Pedido como documento JSON

Um documento por pedido, com cliente, itens e pagamentos embutidos (99.441 documentos). Consultando só os documentos, os totais batem com as tabelas: 112.650 itens e R$ 13.591.643,70.

- **Armadilha encontrada:** nos 775 pedidos sem itens, a lista é `null`, e o `json_each` devolve uma linha nula em vez de nenhuma. O `SUM` não percebe, mas o `COUNT(*)` contava 775 itens a mais.
- **Tamanho:** 57,2 MB em JSON contra 44,7 MB dos quatro CSVs (1,28 vez), porque os nomes dos campos se repetem em cada documento.
- **Embutir o cliente:** 250 pessoas usaram mais de um CEP. O caso mais extremo alterna entre três CEPs da mesma cidade, o que indica endereços de entrega diferentes, e não mudança de endereço. Isso sustenta a hipótese de que o endereço é um fato do pedido, e de que o `customer_id` novo a cada compra congela o endereço de cada entrega.

### Modelo proposto

| Tabela | PK | Grão |
|---|---|---|
| customers | `customer_id` | O cliente num pedido (endereço de entrega daquela compra) |
| orders | `order_id` | Um pedido |
| items | (`order_id`, `order_item_id`) | Uma unidade vendida |
| payments | (`order_id`, `payment_sequential`) | Um meio de pagamento usado no pedido |
| reviews | (`review_id`, `order_id`) | A associação de uma avaliação a um pedido |
| products | `product_id` | Um produto do catálogo |
| sellers | `seller_id` | Um vendedor |
| geolocation → `t_cep` | `zip_prefix` | Um prefixo de CEP |

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
