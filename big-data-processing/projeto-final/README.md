# Pipeline Medallion — ShopBrasil / DataFlow Analytics

Projeto Final da disciplina **Big Data Processing** — MBA em Engenharia de Dados, Universidade Presbiteriana Mackenzie.
Professor: Alexandre Tavares. **Opção A — Pipeline de E-commerce.**

Pipeline de produção que ingere vendas de múltiplas fontes, aplica arquitetura
Medallion, valida qualidade com quarentena funcional e publica tabelas agregadas
para o dashboard executivo. Tudo orquestrado por Airflow e containerizado.

## Integrantes

| Nome completo |
|---|
| Rafael Cardoso Nascimento |
| David Pereira da Silva |
| Matheus Alves da Silva |

## Resultado de uma execução completa

| Camada | Volume |
|---|---|
| Bronze | 1.551.501 registros (4 fontes, 3 formatos) |
| Silver | 1.050.101 vendas · 500.000 clientes · 50 categorias |
| Quarentena | 9.277 registros (0,88%) |
| Gold | 3 tabelas agregadas |

Tempo de execução ponta a ponta: **~3min30s** (8 GB RAM, 4 cores).

---

## Como rodar

### Pré-requisitos

- Docker Desktop com Docker Compose v2
- 8 GB de RAM e 4 cores disponíveis para o Docker
- ~10 GB de espaço em disco

### 1. Clonar este repositório

```bash
git clone https://github.com/RafaelCardoso140701/MBA---Mackenzie.git
cd MBA---Mackenzie/big-data-processing/projeto-final
```

### 2. Obter os dados de entrada

Os datasets não são versionados (somam ~75 MB). Eles vêm do repositório da
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

O primeiro build leva 5–10 min (instala Java 17 e PySpark na imagem do Airflow).
O comando sobe tudo sem nenhuma intervenção manual: prepara o volume de dados,
migra o banco do Airflow, cria o usuário admin e inicia os serviços.

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
docker compose down          # mantém os dados processados
docker compose down -v       # remove tudo, inclusive o data lake
```

---

## Arquitetura

```
data/raw  →  BRONZE  →  SILVER  →  [QUALITY GATE]  →  GOLD
 (fontes)   (bruto +   (schema      ├─ aprovada →  (3 tabelas
            metadados)  unificado)   └─ quarentena   agregadas)
```

Diagrama completo e decisões de projeto em [`docs/arquitetura.md`](docs/arquitetura.md).

### Fontes ingeridas

| Fonte | Formato | Registros | Origem |
|---|---|---|---|
| `vendas_master` | Parquet | 1.000.000 | `aula_02/vendas_2023_completo.parquet` |
| `vendas_parceiros` | CSV | 51.500 | `aula_06/dados_sujos/vendas_problemas.csv` |
| `clientes` | Parquet | 500.000 | `aula_02/clientes.parquet` |
| `categorias` | JSON aninhado | 50 (após explode) | `aula_02/categorias.json` |

### Checks de qualidade

Implementação própria em PySpark — o enunciado proíbe Great Expectations e Soda.

| Check | Dimensão | Regra |
|---|---|---|
| `completude_chaves` | Completude | `order_id`, `customer_id`, `order_date` e `total_amount` obrigatórios |
| `unicidade_order_id` | Unicidade | `order_id` identifica um único pedido |
| `validade_valores` | Validade | `quantity` e `total_amount` positivos |
| `validade_dominio_uf_status` | Validade | UF brasileira, status e forma de pagamento em domínio |
| `validade_periodo` | Validade | `order_date` dentro de 2023 |
| `consistencia_total` | Consistência | `total_amount` = `quantity` × `unit_price` (±R$ 0,01) |

Um registro pode violar várias regras: todas são acumuladas na coluna
`_quality_errors`. Os reprovados vão para `data/quarantine/`, particionados por
`_source`, e **não entram em nenhuma métrica de negócio**.

O `checks.py` é um portão: acima do limite de reprovação (`--limite-reprovacao`,
padrão 0,5) ele sai com código 1, a task falha e a Gold não executa.

### Tabelas Gold

| Tabela | Conteúdo |
|---|---|
| `faturamento_por_estado` | Receita, ticket médio, clientes únicos, participação e ranking por UF |
| `vendas_mensais` | Série mensal com variação MoM e receita acumulada |
| `faturamento_por_segmento` | Cruzamento vendas × cadastro de clientes |

Só pedidos `confirmed`, `shipped` e `delivered` entram no faturamento.

---

## Estrutura do repositório

```
projeto-final/
├── README.md                    # Este arquivo
├── docker-compose.yml           # Um comando sobe todo o ambiente
├── Dockerfile.airflow           # Airflow + Java 17 + PySpark
├── requirements.txt
├── dags/
│   └── pipeline.py              # DAG com 6 tasks encadeadas
├── spark_jobs/
│   ├── ingestao.py              # Bronze
│   ├── transformacao.py         # Silver
│   └── agregacao.py             # Gold
├── quality/
│   └── checks.py                # 6 validações + quarentena
├── spark_conf/
│   └── spark-defaults.conf      # Configuração compartilhada do Spark
├── data/
│   └── raw/                     # Entrada (não versionada — ver passo 2)
└── docs/
    ├── arquitetura.md           # Diagrama e decisões de projeto
    └── VALIDACAO.md             # Roteiro de teste em máquina limpa
```

## Stack

| Tecnologia | Versão |
|---|---|
| Python | 3.11 |
| Apache Spark (PySpark) | 3.5.1 |
| Apache Airflow | 2.8.4 |
| Docker Compose | v2 |
| PostgreSQL (metadados Airflow) | 15 |
| Formato de saída | Parquet (snappy) |

## Problemas conhecidos

**`Port 8081 is already allocated`** — o ambiente da disciplina usa a mesma
porta. Derrube-o antes: `docker compose -f shared/docker-compose.full.yml down`
no repositório do professor.

**Avisos `WindowExec: No Partition Defined`** — esperado. As janelas de ranking e
`lag` na Gold operam sobre o resultado já agregado (12 a 27 linhas), não sobre o
volume bruto.
