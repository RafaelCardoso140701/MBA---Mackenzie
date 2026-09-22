# Pipeline Medallion â€” ShopBrasil / DataFlow Analytics

Projeto Final da disciplina **Big Data Processing** â€” MBA em Engenharia de Dados, Universidade Presbiteriana Mackenzie.
Professor: Alexandre Tavares. **OpÃ§Ã£o A â€” Pipeline de E-commerce.**

Pipeline de produÃ§Ã£o que ingere vendas de mÃºltiplas fontes, aplica arquitetura
Medallion, valida qualidade com quarentena funcional e publica tabelas agregadas
para o dashboard executivo. Tudo orquestrado por Airflow e containerizado.

## Integrantes

| Nome completo |
|---|
| _(preencher)_ |
| _(preencher)_ |
| _(preencher)_ |
| _(preencher)_ |

## Resultado de uma execuÃ§Ã£o completa

| Camada | Volume |
|---|---|
| Bronze | 1.551.501 registros (4 fontes, 3 formatos) |
| Silver | 1.050.101 vendas Â· 500.000 clientes Â· 50 categorias |
| Quarentena | 9.277 registros (0,88%) |
| Gold | 3 tabelas agregadas |

Tempo de execuÃ§Ã£o ponta a ponta: **~3min30s** (8 GB RAM, 4 cores).

---

## Como rodar

### PrÃ©-requisitos

- Docker Desktop com Docker Compose v2
- 8 GB de RAM e 4 cores disponÃ­veis para o Docker
- ~10 GB de espaÃ§o em disco

### 1. Clonar este repositÃ³rio

```bash
git clone https://github.com/RafaelCardoso140701/MBA---Mackenzie.git
cd MBA---Mackenzie/big-data-processing/projeto-final
```

### 2. Obter os dados de entrada

Os datasets nÃ£o sÃ£o versionados (somam ~75 MB). Eles vÃªm do repositÃ³rio da
disciplina:

```bash
git clone https://github.com/AleTavares/Mackenzie_BigDataProcessing.git ../../../Mackenzie_BigDataProcessing
```

**Linux / macOS:**

```bash
mkdir -p data/raw/aula_02 data/raw/aula_06/dados_sujos
cp ../../../Mackenzie_BigDataProcessing/datasets/aula_02/* data/raw/aula_02/
cp ../../../Mackenzie_BigDataProcessing/datasets/aula_06/dados_sujos/* data/raw/aula_06/dados_sujos/
```

**Windows (PowerShell):**

```powershell
mkdir data\raw\aula_02, data\raw\aula_06\dados_sujos -Force
Copy-Item ..\..\..\Mackenzie_BigDataProcessing\datasets\aula_02\* data\raw\aula_02\ -Recurse -Force
Copy-Item ..\..\..\Mackenzie_BigDataProcessing\datasets\aula_06\dados_sujos\* data\raw\aula_06\dados_sujos\ -Recurse -Force
```

### 3. Subir o ambiente

```bash
docker compose up -d --build
```

O primeiro build leva 5â€“10 min (instala Java 17 e PySpark na imagem do Airflow).
O comando sobe tudo sem nenhuma intervenÃ§Ã£o manual: prepara o volume de dados,
migra o banco do Airflow, cria o usuÃ¡rio admin e inicia os serviÃ§os.

### 4. Executar o pipeline

**Pela interface**, em http://localhost:8081 (login `admin` / `admin`):
abra a DAG `pipeline_medallion_shopbrasil` e clique em **Trigger DAG**.

**Ou pelo terminal:**

```bash
docker compose exec airflow-scheduler airflow dags test pipeline_medallion_shopbrasil 2024-01-01
```

### 5. Conferir os resultados

```bash
docker compose exec airflow-scheduler cat /opt/airflow/data/reports/ultima_execucao.txt
docker compose exec airflow-scheduler cat /opt/airflow/data/reports/quality_report.json
docker compose exec airflow-scheduler ls -R /opt/airflow/data/gold
```

### 6. Encerrar

```bash
docker compose down          # mantÃ©m os dados processados
docker compose down -v       # remove tudo, inclusive o data lake
```

---

## Arquitetura

```
data/raw  â†’  BRONZE  â†’  SILVER  â†’  [QUALITY GATE]  â†’  GOLD
 (fontes)   (bruto +   (schema      â”œâ”€ aprovada â†’  (3 tabelas
            metadados)  unificado)   â””â”€ quarentena   agregadas)
```

Diagrama completo e decisÃµes de projeto em [`docs/arquitetura.md`](docs/arquitetura.md).

### Fontes ingeridas

| Fonte | Formato | Registros | Origem |
|---|---|---|---|
| `vendas_master` | Parquet | 1.000.000 | `aula_02/vendas_2023_completo.parquet` |
| `vendas_parceiros` | CSV | 51.500 | `aula_06/dados_sujos/vendas_problemas.csv` |
| `clientes` | Parquet | 500.000 | `aula_02/clientes.parquet` |
| `categorias` | JSON aninhado | 50 (apÃ³s explode) | `aula_02/categorias.json` |

### Checks de qualidade

ImplementaÃ§Ã£o prÃ³pria em PySpark â€” o enunciado proÃ­be Great Expectations e Soda.

| Check | DimensÃ£o | Regra |
|---|---|---|
| `completude_chaves` | Completude | `order_id`, `customer_id`, `order_date` e `total_amount` obrigatÃ³rios |
| `unicidade_order_id` | Unicidade | `order_id` identifica um Ãºnico pedido |
| `validade_valores` | Validade | `quantity` e `total_amount` positivos |
| `validade_dominio_uf_status` | Validade | UF brasileira, status e forma de pagamento em domÃ­nio |
| `validade_periodo` | Validade | `order_date` dentro de 2023 |
| `consistencia_total` | ConsistÃªncia | `total_amount` = `quantity` Ã— `unit_price` (Â±R$ 0,01) |

Um registro pode violar vÃ¡rias regras: todas sÃ£o acumuladas na coluna
`_quality_errors`. Os reprovados vÃ£o para `data/quarantine/`, particionados por
`_source`, e **nÃ£o entram em nenhuma mÃ©trica de negÃ³cio**.

O `checks.py` Ã© um portÃ£o: acima do limite de reprovaÃ§Ã£o (`--limite-reprovacao`,
padrÃ£o 0,5) ele sai com cÃ³digo 1, a task falha e a Gold nÃ£o executa.

### Tabelas Gold

| Tabela | ConteÃºdo |
|---|---|
| `faturamento_por_estado` | Receita, ticket mÃ©dio, clientes Ãºnicos, participaÃ§Ã£o e ranking por UF |
| `vendas_mensais` | SÃ©rie mensal com variaÃ§Ã£o MoM e receita acumulada |
| `faturamento_por_segmento` | Cruzamento vendas Ã— cadastro de clientes |

SÃ³ pedidos `confirmed`, `shipped` e `delivered` entram no faturamento.

---

## Estrutura do repositÃ³rio

```
projeto-final/
â”œâ”€â”€ README.md                    # Este arquivo
â”œâ”€â”€ docker-compose.yml           # Um comando sobe todo o ambiente
â”œâ”€â”€ Dockerfile.airflow           # Airflow + Java 17 + PySpark
â”œâ”€â”€ requirements.txt
â”œâ”€â”€ dags/
â”‚   â””â”€â”€ pipeline.py              # DAG com 6 tasks encadeadas
â”œâ”€â”€ spark_jobs/
â”‚   â”œâ”€â”€ ingestao.py              # Bronze
â”‚   â”œâ”€â”€ transformacao.py         # Silver
â”‚   â””â”€â”€ agregacao.py             # Gold
â”œâ”€â”€ quality/
â”‚   â””â”€â”€ checks.py                # 6 validaÃ§Ãµes + quarentena
â”œâ”€â”€ spark_conf/
â”‚   â””â”€â”€ spark-defaults.conf      # ConfiguraÃ§Ã£o compartilhada do Spark
â”œâ”€â”€ data/
â”‚   â””â”€â”€ raw/                     # Entrada (nÃ£o versionada â€” ver passo 2)
â””â”€â”€ docs/
    â”œâ”€â”€ arquitetura.md           # Diagrama e decisÃµes de projeto
    â””â”€â”€ VALIDACAO.md             # Roteiro de teste em mÃ¡quina limpa
```

## Stack

| Tecnologia | VersÃ£o |
|---|---|
| Python | 3.11 |
| Apache Spark (PySpark) | 3.5.1 |
| Apache Airflow | 2.8.4 |
| Docker Compose | v2 |
| PostgreSQL (metadados Airflow) | 15 |
| Formato de saÃ­da | Parquet (snappy) |

## Problemas conhecidos

**`Port 8081 is already allocated`** â€” o ambiente da disciplina usa a mesma
porta. Derrube-o antes: `docker compose -f shared/docker-compose.full.yml down`
no repositÃ³rio do professor.

**Avisos `WindowExec: No Partition Defined`** â€” esperado. As janelas de ranking e
`lag` na Gold operam sobre o resultado jÃ¡ agregado (12 a 27 linhas), nÃ£o sobre o
volume bruto.

