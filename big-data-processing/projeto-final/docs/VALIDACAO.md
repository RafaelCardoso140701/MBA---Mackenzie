# Roteiro de Validação

Para cada integrante do grupo rodar o pipeline na própria máquina, do zero, e
confirmar que os resultados batem.

**Objetivo:** provar que o `docker compose up` funciona em máquina limpa — é
exatamente o que o professor vai fazer, e vale 30% da nota.

Reserve 40 minutos. O grosso é espera de download.

---

## Antes de começar

- [ ] Docker Desktop instalado e aberto
- [ ] Em Settings → Resources: **mínimo 8 GB de RAM e 4 CPUs**
      (no Windows com WSL2, isso se configura em `%USERPROFILE%\.wslconfig`)
- [ ] ~10 GB de espaço livre em disco
- [ ] Git instalado

> Se você já rodou o ambiente da disciplina, **derrube antes** — ele ocupa a
> porta 8081:
> ```
> docker compose -f shared/docker-compose.full.yml down
> ```

---

## Passo 1 — Clonar

```bash
git clone https://github.com/RafaelCardoso140701/MBA---Mackenzie.git
cd MBA---Mackenzie/big-data-processing/projeto-final
```

✅ **Validar:** `ls` (ou `dir`) mostra `docker-compose.yml`, `dags/`,
`spark_jobs/`, `quality/`.

---

## Passo 2 — Baixar os dados

Os datasets não são versionados. Clone o repositório da disciplina ao lado deste:

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

✅ **Validar:** o arquivo `data/raw/aula_02/vendas_2023_completo.parquet` existe e
tem ~54 MB.

---

## Passo 3 — Construir e subir

```bash
docker compose up -d --build
```

O build instala Java 17 e PySpark sobre a imagem do Airflow. **5 a 10 minutos na
primeira vez.** Espere o prompt voltar.

```bash
docker compose ps -a
```

✅ **Validar:**

| Container | Estado esperado |
|---|---|
| `pf-data-init` | `Exited (0)` |
| `pf-airflow-init` | `Exited (0)` |
| `pf-postgres` | `Up (healthy)` |
| `pf-airflow-webserver` | `Up` |
| `pf-airflow-scheduler` | `Up` |

Os dois `Exited (0)` são corretos: são serviços de inicialização que rodam uma
vez e encerram.

❌ Se `pf-data-init` não saiu com 0: `docker compose logs data-init`

---

## Passo 4 — Conferir o ambiente

```bash
docker compose exec airflow-scheduler java -version
docker compose exec airflow-scheduler spark-submit --version
docker compose exec airflow-scheduler airflow connections get spark_local
docker compose exec airflow-scheduler airflow connections get fs_default
```

✅ **Validar:** Java 17, Spark 3.5.1 e as duas conexões listadas.

---

## Passo 5 — Abrir o Airflow

http://localhost:8081 — login `admin` / `admin`

✅ **Validar:** a DAG `pipeline_medallion_shopbrasil` aparece e está
**despausada** (o botão à esquerda deve estar azul, não cinza). Pode levar até
30 s para ser detectada.

---

## Passo 6 — Executar

Pela interface: clique na DAG → botão ▶ (Trigger DAG) → acompanhe em **Graph**.

Ou pelo terminal, que mostra tudo de uma vez:

```bash
docker compose exec airflow-scheduler airflow dags test pipeline_medallion_shopbrasil 2024-01-01
```

Leva **3 a 6 minutos**. As seis tasks devem ficar verdes na ordem:
`aguardar_dados` → `ingestao_bronze` → `transformacao_silver` →
`quality_checks` → `agregacao_gold` → `notificar_conclusao`.

---

## Passo 7 — Validar os números

Confira contra a tabela abaixo. **Os valores têm que bater exatamente** — o
pipeline é determinístico.

### Bronze

```bash
docker compose exec airflow-scheduler python -c "
from pyspark.sql import SparkSession
s = SparkSession.builder.master('local[*]').appName('validacao').getOrCreate()
for t in ['vendas_master','vendas_parceiros','clientes','categorias']:
    print(t, s.read.parquet('/opt/airflow/data/bronze/'+t).count())
"
```

| Tabela | Esperado |
|---|---|
| `vendas_master` | 1000000 |
| `vendas_parceiros` | 51500 |
| `clientes` | 500000 |
| `categorias` | 1 |

### Silver, quarentena e Gold

```bash
docker compose exec airflow-scheduler python -c "
from pyspark.sql import SparkSession
s = SparkSession.builder.master('local[*]').appName('validacao').getOrCreate()
base = '/opt/airflow/data'
for cam, t in [('silver','vendas'),('silver','vendas_aprovada'),('silver','clientes'),
               ('silver','categorias'),('quarantine','vendas_reprovada'),
               ('gold','faturamento_por_estado'),('gold','vendas_mensais'),
               ('gold','faturamento_por_segmento')]:
    print(f'{cam}/{t}', s.read.parquet(f'{base}/{cam}/{t}').count())
"
```

| Tabela | Esperado |
|---|---|
| `silver/vendas` | 1050101 |
| `silver/vendas_aprovada` | 1040824 |
| `silver/clientes` | 500000 |
| `silver/categorias` | 50 |
| `quarantine/vendas_reprovada` | 9277 |
| `gold/faturamento_por_estado` | 27 |
| `gold/vendas_mensais` | 12 |
| `gold/faturamento_por_segmento` | 4 |

### Relatório de qualidade

```bash
docker compose exec airflow-scheduler cat /opt/airflow/data/reports/quality_report.json
```

| Check | Falhas esperadas |
|---|---|
| `completude_chaves` | 2507 |
| `unicidade_order_id` | 202 |
| `validade_valores` | 494 |
| `validade_dominio_uf_status` | 5268 |
| `validade_periodo` | 404 |
| `consistencia_total` | 1559 |

Taxa de reprovação: **0,0088** (0,88%).

### Resumo da execução

```bash
docker compose exec airflow-scheduler cat /opt/airflow/data/reports/ultima_execucao.txt
```

---

## Passo 8 — Validar a quarentena

Este é o item que o professor mais deve querer ver.

```bash
docker compose exec airflow-scheduler ls /opt/airflow/data/quarantine/vendas_reprovada/
```

✅ **Validar:** aparecem as pastas `_source=vendas_master` e
`_source=vendas_parceiros` — a quarentena é particionada pela origem.

```bash
docker compose exec airflow-scheduler python -c "
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
s = SparkSession.builder.master('local[*]').appName('quarentena').getOrCreate()
q = s.read.parquet('/opt/airflow/data/quarantine/vendas_reprovada')
q.groupBy('_source').count().show()
q.select('order_id','quantity','total_amount','shipping_state','status','_quality_errors').show(10, False)
"
```

✅ **Validar:** registros com dois ou mais motivos no array `_quality_errors`, e
o par de `order_id` duplicado com `total_amount` diferente.

---

## Passo 9 — Validar o portão de qualidade (opcional, mas vale a pena)

Demonstra que o pipeline **trava** com dado ruim demais:

```bash
docker compose exec airflow-scheduler spark-submit --master "local[*]" --driver-memory 2g \
  /opt/airflow/quality/checks.py --limite-reprovacao 0.001
```

✅ **Validar:** o job registra `Taxa de reprovação 0.88% acima do limite de 0.10%.
Pipeline interrompido.` e sai com código 1.

---

## Passo 10 — Encerrar

```bash
docker compose down -v
```

O `-v` remove os volumes. Use quando quiser repetir a validação do zero.

---

## Ficha de validação

Preencha e mande no grupo:

```
Integrante: ______________________
Sistema operacional: _____________
RAM/CPU alocados ao Docker: ______

[ ] Passo 3  — cinco containers no estado esperado
[ ] Passo 4  — Java 17, Spark 3.5.1 e as duas conexões
[ ] Passo 5  — DAG visível e despausada
[ ] Passo 6  — seis tasks verdes
[ ] Passo 7  — todos os números bateram
[ ] Passo 8  — quarentena particionada por _source
[ ] Passo 9  — portão de qualidade trava o pipeline

Tempo do build: ______ min
Tempo da execução da DAG: ______ min

Divergências ou erros encontrados:
_________________________________________________
```

---

## Se algo falhar

| Sintoma | Causa provável | O que fazer |
|---|---|---|
| `Port 8081 is already allocated` | Ambiente da disciplina no ar | `docker compose -f shared/docker-compose.full.yml down` no repositório do professor |
| Build falha baixando pacotes | Rede instável | Repetir `docker compose build` |
| `data-init` não sai com 0 | Permissão do volume | `docker compose down -v` e subir de novo |
| Task Spark morre com `OutOfMemoryError` | Pouca RAM no Docker | Aumentar para 8 GB em Settings → Resources |
| DAG não aparece na interface | Ainda não foi detectada | Aguardar 30 s e atualizar; se persistir, `docker compose logs airflow-scheduler` |
| Números não batem | Dados de entrada incompletos | Conferir o passo 2 e o tamanho dos arquivos |

Para qualquer outro erro: copie o **trecho do `Traceback`** (não o log inteiro do
Spark) e mande no grupo.
