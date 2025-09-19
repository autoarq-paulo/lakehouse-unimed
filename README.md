# Lakehouse Unimed – Data & AI Platform (Iceberg + Delta + Trino + Spark + Airflow)

Este repositório contém uma implementação de **Lakehouse** com foco em governança e simplicidade operacional,
combinando **Apache Iceberg**, **Delta Lake**, **Trino**, **Apache Spark**, **Airflow**, **MinIO** (S3 compatível),
**Prometheus** e **Grafana**.

> Objetivo: oferecer um acelerador de arquitetura com ingestão Bronze (Delta), padronização Silver (Iceberg),
> e publicação Gold (views no Trino), com observabilidade básica.

---

## Visão Geral

- **Ingestão (Bronze)**: arquivos CSV são ingeridos para **Delta Lake** via Spark.
- **Padronização (Silver)**: dados bronze são limpos/enriquecidos e publicados em **Iceberg**.
- **Consumo (Gold)**: **Trino** cria *views* em cima das tabelas Iceberg.
- **Orquestração**: **Airflow** agenda e executa jobs Spark e comandos Trino.
- **Armazenamento**: **MinIO** como *object store* compatível com S3.
- **Metastore**: Hive Metastore (para Iceberg).
- **Observabilidade**: **Prometheus** + **Grafana** (datasource preparado).

---

## Estrutura de Diretórios

```
dags/
  bronze_ingest_csv.py
  silver_claims_transform.py
  gold_publish_views.py

jobs/
  bronze/ingest_csv_to_delta.py        # (já criado previamente no repo)
  silver/claims_to_iceberg.py

sql/
  gold/create_views.sql
  trino_checks.sql

dq/
  expectations/silver_claims.json

deploy/
  grafana/datasources/datasource.yml
  prometheus/prometheus.yml            # (seu deploy inclui Prometheus)
  jmx/{spark.yml,trino.yml}            # (opcionais para métricas JMX)

compose/
  base.yml                             # serviços principais (Spark, Trino, MinIO, Metastore, Airflow, etc.)
  metrics.yml                          # Prometheus, Node Exporter, cAdvisor
```

> Observação: os arquivos `compose/` não são gerados por este README, mas devem existir no repositório (você já subiu alguns).

---

## Pré‑requisitos

- **Docker** e **Docker Compose** instalados.
- Criar um arquivo **.env** (local, **não versão**) com variáveis reais.
- O repositório inclui **.env.example** para referência e **.gitignore** para evitar *leak* de segredos.

Variáveis típicas usadas pelos serviços e jobs:

```
PG_DB=example_db
PG_USER=example_user
PG_PASSWORD=example_password

MINIO_ROOT_USER=example_minio_user
MINIO_ROOT_PASSWORD=example_minio_pass
S3_ENDPOINT=http://minio:9000
S3_REGION=us-east-1

TRINO_CATALOG=lake
```

---

## Subindo a stack

1) Ajuste o `.env` com valores reais (não comite este arquivo).  
2) Suba os serviços base e, opcionalmente, as métricas:

```bash
# Base (Spark, Trino, Metastore, MinIO, Airflow, etc.)
docker compose -f compose/base.yml up -d

# Observabilidade (Prometheus, cAdvisor, Node Exporter, Grafana)
docker compose -f compose/base.yml -f compose/metrics.yml up -d
```

Aguarde a saúde dos containers (Metastore, Trino, Spark) antes de rodar pipelines.

---

## Principais Endpoints (padrões comuns)

- **Trino**: `http://localhost:8080`  
- **MinIO Console**: `http://localhost:9001` (S3 em `http://localhost:9000`)  
- **Airflow Webserver**: ex.: `http://localhost:8081` (conforme seu compose)  
- **Prometheus**: `http://localhost:9090`  
- **Grafana**: `http://localhost:3000`

> Portas podem variar conforme seu `compose/*.yml`. Ajuste conforme necessário.

---

## Airflow – Configuração Rápida

No Airflow, defina variáveis (Admin → Variables) para que os DAGs possam ler credenciais do MinIO:

- `MINIO_ROOT_USER`
- `MINIO_ROOT_PASSWORD`

Alternativamente, mapeie as variáveis de ambiente no container do Airflow via `compose/base.yml`.

---

## Pipelines

### Bronze – Ingestão CSV → Delta
- DAG: `dags/bronze_ingest_csv.py`
- Job Spark: `jobs/bronze/ingest_csv_to_delta.py`
- Comando (executado pelo DAG):
  - lê `/data/incoming/claims/*.csv`
  - escreve em `s3a://lake/bronze/claims` como **Delta**

### Silver – Padronização → Iceberg
- DAG: `dags/silver_claims_transform.py`
- Job Spark: `jobs/silver/claims_to_iceberg.py`
- Lê `s3a://lake/bronze/claims` (Delta), faz limpezas e publica em **Iceberg** (`lake.silver.claims`),
  além de espelhar *dataset* silver em Delta (`s3a://lake/silver/claims`).

### Gold – Views no Trino
- DAG: `dags/gold_publish_views.py`
- SQL: `sql/gold/create_views.sql`
- Cria/atualiza `lake.gold.claims_summary` para consumo.

---

## Trino – Verificações Rápidas

O arquivo `sql/trino_checks.sql` contém comandos para *smoke test*:

- `SHOW CATALOGS;`
- `SHOW SCHEMAS FROM lake;`
- `SHOW TABLES FROM lake.silver;`
- `SELECT * FROM lake.silver.claims LIMIT 5;`
- `SELECT * FROM lake.gold.claims_summary LIMIT 5;`

Execute no CLI do Trino dentro do container ou via JDBC.

---

## Data Quality (exemplo)

`dq/expectations/silver_claims.json` ilustra *checks* mínimos (null, tipo, faixa).  
Integrações com **Great Expectations** ou **Soda** podem ser adicionadas futuramente.

---

## Observabilidade

- `compose/metrics.yml` sobe **Prometheus**, **Node Exporter** e **cAdvisor**.
- `deploy/grafana/datasources/datasource.yml` já configura o datasource Prometheus no Grafana.
- Ajuste dashboards conforme necessidade (ex.: métricas de Spark/Trino via JMX Exporter).

---

## Segurança e Segredos

- **Nunca** comite segredos (senhas, chaves) no repositório.
- `.gitignore` já ignora `.env` e arquivos sensíveis.
- Use **.env** local e variáveis do Airflow (**Admin → Variables**) para credenciais.

---

## Troubleshooting

- **Metastore não sobe**: confirme variáveis `PG_*` e conectividade com o Postgres.
- **Spark não acessa S3**: valide `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD` e `S3_ENDPOINT`.
- **Trino não enxerga catálogos**: revise `deploy/trino/etc/catalog/*.properties`.
- **Permissões no MinIO**: crie *bucket* `lake` e garanta política de acesso compatível.
- **Airflow não encontra jobs**: monte `/opt/jobs` e `/opt/sql` corretamente no `compose`.

---

## Roadmap (sugestões)

- Particionamento e ordenação (Iceberg) por colunas de alto *cardinality* ou *time-based*.
- Retenção/otimização: **Iceberg snapshots**, `VACUUM` Delta, **compaction**.
- Catálogo unificado (ex.: **Unity Catalog** ou metastore compartilhado para governança).
- DQ avançado com **Great Expectations** + *data docs*.
- Alertas Grafana/Prometheus + *SLOs* de pipelines.

---

## Como contribuir

1. Crie uma *branch* feature (`git checkout -b feature/nome`).
2. *Commit* claro: `feat(dags): add silver job for ...`.
3. Abra *PR* com descrição do impacto e *rollout plan*.

---

## Licença

Defina a licença conforme a política da sua organização (MIT/Apache-2.0/etc.).

---

### Mensagem de commit sugerida para este README

```
Docs: add comprehensive README (architecture, setup, pipelines, ops)
```