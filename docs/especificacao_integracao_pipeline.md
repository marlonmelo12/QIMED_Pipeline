# 📘 QIMED Health Lakehouse — Especificação de Integração, Orquestração & Espelhamento

Este documento consolida todas as diretrizes técnicas, arquiteturais e contratos de dados para a integração entre o **QIMED Lakehouse / Airflow** e o **Backend / Frontend da Aplicação**.

---

## 📑 Resumo Executivo das Diretrizes de Integração

### 1. Camada do Medalhão e Grau de Tratamento
* **Para a Tela / Dashboards (Consumo Imediato):** Consome a **Camada Gold** (`dm_ans_glosas_operadoras`, `dm_glosas_auditoria`, `aud_alertas_anomalias`).
* **Para Arquivo Morto / Auditoria Histórica no seu Banco:** O worker espelha a **Camada Silver** (`fct_tiss_glosas_operadoras` e `dim_operadoras_saude`).
* **Grau de Tratamento:** O dado chega **100% TRATADO, ENRIQUECIDO E PRÉ-AGREGADO**. Não há necessidade de joins pesados, cálculos de proporções ou limpezas em tempo de requisição na API.

---

### 2. Nomes de Tabelas e Colunas para as Tabelas Espelho
A tabela principal a ser criada no seu banco relacional é a **`dm_ans_glosas_operadoras`**:

| Coluna | Tipo de Dado | Descrição / Exemplo |
|---|---|---|
| `id_registro_kpi` | `VARCHAR(64)` | **Chave Primária (PK)** — SHA-256 da chave composta. |
| `codigo_registro_ans` | `VARCHAR(10)` | Código da operadora na ANS (ex.: `'005711'`). |
| `cnpj_operadora` | `VARCHAR(18)` | CNPJ formatado da operadora. |
| `razao_social` | `VARCHAR(255)` | Razão social oficial da operadora. |
| `operadora_label` | `VARCHAR(300)` | Texto pronto para dropdowns (ex.: `'005711 - BRADESCO SAÚDE S.A.'`). |
| `porte_operadora` | `VARCHAR(20)` | `'Pequeno'`, `'Medio'`, `'Grande'`. |
| `modalidade_operadora` | `VARCHAR(50)` | `'Cooperativa Medica'`, `'Medicina de Grupo'`, `'Autogestao'`, etc. |
| `segmentacao_assistencial`| `VARCHAR(50)` | `'Ambulatorial'`, `'Hospitalar c/ Obstetricia'`, etc. |
| `ano` / `mes` / `periodo` | `VARCHAR` | `'2026'`, `'05'`, `'2026-05'`. |
| `tempo_medio_pagamento_dias` | `DOUBLE` | Tempo médio em dias corridos até o pagamento. |
| `taxa_glosa_inicial_pct` | `DOUBLE` | Percentual de glosa inicial (escala $0$ a $100$, 2 casas decimais). |
| `taxa_glosa_final_pct` | `DOUBLE` | Percentual de glosa final pós-recurso (2 casas decimais). |
| `pct_guias_sem_retorno_60d` | `DOUBLE` | % do volume físico de guias sem retorno em $> 60$ dias. |
| `pct_valor_guias_sem_retorno_60d` | `DOUBLE` | % do montante ($R\$$) sem retorno em $> 60$ dias. |
| `total_faturado_brl` | `DOUBLE` | Numerador / Denominador: Valor total faturado ($R\$$). |
| `total_glosado_final_brl` | `DOUBLE` | Numerador / Denominador: Valor glosado final ($R\$$). |
| `is_operadora_atipica` | `BOOLEAN` | `TRUE` se a operadora concentra $\ge 90\%$ da glosa do setor. |
| `motivo_atipicidade` | `VARCHAR(255)` | Justificativa do alerta exibido no Card da UI. |
| `dt_carga` | `TIMESTAMP` | Watermark da data de cálculo para carga incremental. |
| `id_execucao` | `VARCHAR(64)` | ID da execução/DAG Run no Airflow. |

---

### 3. Chave de Upsert e Estratégia de Carga (Incremental vs Full)
* **Chave Natural Composta para UPSERT:**
  $$\text{Chave Natural} = \langle\text{codigo\_registro\_ans}, \text{ano}, \text{mes}, \text{modalidade\_operadora}, \text{segmentacao\_assistencial}\rangle$$
* **Estratégia de Carga:** **Incremental via Watermark**.
  * O worker não varre a tabela inteira. Ele busca apenas: `WHERE dt_carga > :ultimo_watermark_sincronizado`.
  * Executa o `INSERT ... ON CONFLICT (...) DO UPDATE` atualizando as métricas sem gerar duplicatas.

---

### 4. Decisão: ORM vs Stored Procedure por Tela
* **Decisão:** **100% ORM e Lógica em Services da Aplicação — Zero Stored Procedures.**
  1. **Telas Padrão (Cards Gerais, Tabelas e Central de Anomalias):** Leitura direta via **ORM** simples (`SELECT` com filtros normais).
  2. **Telas com Filtros Combinados (Multi-Select):** O **Service do Backend** faz a soma dos numeradores e denominadores para computar a média ponderada exata $\left(\frac{\sum \text{glosa}}{\sum \text{faturamento}}\right)$.
  3. **Card de Operadora Atípica & Média Ajustada (Trimmed Mean):** O **Service do Backend** verifica se há uma operadora com $\ge 90\%$ da glosa, gera o card de alerta na UI e subtrai essa operadora do cálculo da média setorial dos cards de topo.

---

### 5. Mecanismo de Disparo e Acompanhamento da Execução
* **Disparo:** Chamada HTTP REST na API do Airflow:
  * `POST http://<airflow-host>:8088/api/v1/dags/{dag_id}/dagRuns`
  * Body: `{"conf": {"uf": "CE", "year": 2026, "month": 5}}`
* **Acompanhamento de Status:**
  * **Notificação Reativa (Recomendada):** A última tarefa da DAG dispara um Webhook `POST /api/v1/sync/mirror-trigger` para o seu backend assim que os dados são comitados.
  * **Polling de Status:** `GET http://<airflow-host>:8088/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}`.
  * **Telemetria Contínua (0% a 100%):** Leitura do arquivo `lakehouse/pipeline_progress.json`.

---

### 6. Credenciais e Acesso ao Banco e Orquestrador
* **Airflow REST API & Webserver:**
  * **URL:** `http://localhost:8088` (externo) ou `http://airflow-webserver:8080` (na rede Docker interna).
  * **Usuário:** `admin` | **Senha:** `admin`
  * **Header HTTP:** `Authorization: Basic YWRtaW46YWRtaW4=`
* **Storage do Lakehouse / Data Warehouse:**
  * **DuckDB:** `warehouse/qimed_dw.duckdb` (leitura relacional OLAP rápida via `duckdb.connect(..., read_only=True)`).
  * **Delta Lake:** `lakehouse/silver/` (arquivos Parquet transacionais ACID).
* **Segredos e LGPD:** Gerenciados via variável de ambiente `QIMED_MPI_SALT` (utilizada na pseudonimização determinística de pacientes).

---

## 🏛️ 1. Detalhamento de Orquestração e Execução do Pipeline

```mermaid
graph LR
    subgraph "Backend / API da Aplicação"
        UI["Tela de Gestão de Pipelines"] --> API["Backend Service"]
    end

    subgraph "Orquestração (Apache Airflow 2.x)"
        API -->|1. POST /dagRuns| AF_REST["Airflow REST API (Porta 8088)"]
        AF_REST --> SCHED["Airflow Scheduler"]
        SCHED --> WORKER["LocalExecutor / Pipeline Master"]
    end

    subgraph "QIMED Lakehouse & DW"
        WORKER --> BRONZE["Camada Bronze (Delta Lake)"]
        BRONZE --> SILVER["Camada Silver (DuckDB / PyArrow)"]
        SILVER --> GOLD["Camada Gold (Data Marts)"]
    end

    WORKER -.->|2. Webhook POST /mirror-trigger| API
```

### 1.1. Qual orquestrador é utilizado?
* **Orquestrador:** **Apache Airflow 2.x** rodando com `LocalExecutor` sobre **PostgreSQL 13**.
* **Status do Ambiente:** Conteinerizado no arquivo [`docker-compose.yml`](file:///c:/Users/marlo/Downloads/QIMED/docker-compose.yml).
* **Comando para subir:** `docker compose up -d` na raiz do projeto.

### 1.2. Como o backend dispara uma execução?
* **Padrão Oficial (REST API):**
  * **Endpoint:** `POST http://<airflow-host>:8088/api/v1/dags/{dag_id}/dagRuns`
  * **Headers:** `Content-Type: application/json`, `Authorization: Basic YWRtaW46YWRtaW4=`
  * **Payload:**
    ```json
    {
      "dag_run_id": "exec_backend_20260828_01",
      "conf": {
        "uf": "CE",
        "year": 2026,
        "month": 5
      }
    }
    ```
* **DAGs Disponíveis:**
  * `qimed_qimed_end_to_end` (Pipeline Fim a Fim)
  * `qimed_datasus_sih` (Internações SUS)
  * `qimed_datasus_cnes` (Estabelecimentos de Saúde)
  * `qimed_datasus_sia` (Produção Ambulatorial)
  * `qimed_ans_supplementary_health` (Saúde Suplementar & Ressarcimento)
  * `qimed_fhir_synthetic` (Dados Clínicos Sintéticos FHIR R4)

---

## 📊 2. Acompanhamento de Status, Logs e Telemetria

### 2.1. Identificador de Execução (`dag_run_id`)
* **Retorno:** A API devolve o `dag_run_id`. Ele é **100% estável e imutável** (chave primária no PostgreSQL do Airflow).
* **Dica:** O backend pode passar seu próprio UUID no campo `"dag_run_id"` no momento do disparo.

### 2.2. Mapeamento de Estados (Airflow $\rightarrow$ Frontend UI)

| Estado no Airflow (`state`) | Estado na Tela | Significado Operacional |
|---|---|---|
| `"queued"` ou `"scheduled"` | **agendado** | Job na fila aguardando slot de processamento. |
| `"running"` | **rodando** | Ingestão / transformação em execução ativa. |
| `"success"` | **concluído** | Pipeline concluído com sucesso e dados gravados. |
| `"failed"` | **com falha** | Falha irrecuperável em uma das tarefas. |
| `"upstream_failed"` | **com falha** | Tarefa anterior falhou, abortando o fluxo. |

### 2.3. Consulta de Status e Logs
* **Polling de Status:** `GET http://<airflow-host>:8088/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}`
* **Logs da Tarefa via API:** `GET http://<airflow-host>:8088/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{try_number}`
* **Telemetria de Progresso em Tempo Real (0% a 100%):** Arquivo [`lakehouse/pipeline_progress.json`](file:///c:/Users/marlo/Downloads/QIMED/src/pipeline/master_pipeline.py#L49-L65).

### 2.4. Políticas de Retry
* **Airflow DAGs:** `retries: 3`, `retry_delay: timedelta(minutes=5)`.
* **Coletor DATASUS (FTP/HTTP):** *Circuit Breaker* com desativação temporária após 3 falhas consecutivas.
* **Storage DuckDB:** Retries com *backoff exponencial* (0.5s, 1s, 2s) em `CHECKPOINT` e `VACUUM`.

---

## ⏱️ 3. Agendamento, Concorrência e Metadados

### 3.1. Fonte da Verdade do Agendamento
* **Banco do Backend:** É a **fonte da verdade da intenção de negócio do usuário**.
* **Airflow:** É o **motor executor**. O backend pode pausar/ativar DAGs via `PATCH /api/v1/dags/{dag_id}` com `{"is_paused": true | false}` ou disparar via API no horário desejado.

### 3.2. Concorrência e Execuções Simultâneas
* **Proteção Nativa por Kernel Lock:** O [`PartitionLockManager`](file:///c:/Users/marlo/Downloads/QIMED/src/ingestion/lock_manager.py) usa `os.open(O_CREAT | O_EXCL)`. Se duas execuções tentarem gravar a mesma UF/Mês/Ano simultaneamente, a segunda é rejeitada com segurança, **sem risco de corrupção**.
* **Airflow:** DAGs configuradas com `max_active_runs=1`.

### 3.3. Metadados de Origem $\rightarrow$ Destino

| Pipeline | Origem (Fonte Oficial) | Destino Silver / Gold (Lakehouse & DW) |
|---|---|---|
| **Internações SUS (SIH)** | `FTP DATASUS (/SIHSUS/Dados/RD*.dbc)` | `fct_internacao` $\rightarrow$ `vw_internacoes_consolidadas` |
| **Glosas & Rejeições (SIH-RJ/ER)** | `FTP DATASUS (/SIHSUS/Dados/RJ*.dbc e ER*.dbc)` | `fct_glosas_hospitalares` $\rightarrow$ `dm_glosas_auditoria` |
| **Ambulatório SUS (SIA)** | `FTP DATASUS (/SIASUS/Dados/PA*.dbc)` | `fct_atendimentos_ambulatoriais` |
| **Estabelecimentos (CNES)** | `FTP DATASUS (/CNESUS/Dados/ST*.dbc)` | `dim_estabelecimento` |
| **Saúde Suplementar (ANS)** | `HTTP Dados Abertos ANS (Cadop / ABI)` | `dim_operadoras_saude` $\rightarrow$ `fct_ressarcimento_sus` |
| **Atenção Primária (SISAB)** | `API e-SUS APS (SISAB/JSON)` | `dm_icsap_prevention` |
| **Regulação & Filas (SISREG)** | `Exportação / API SISREG (CSV)` | `fct_regulacao_filas` $\rightarrow$ `dm_regulation_bottlenecks` |
| **Clínico Sintético (FHIR R4)** | `Gerador Sintético FHIR R4 (JSON Bundles)` | `dim_patients` $\rightarrow$ `fct_encounters` |

---

## 🔄 4. Worker de Espelhamento & Notificação Reativa (Webhook)

### 4.1. Mecanismo de Disparo Reativo (IMPLEMENTADO ✅)
Para garantir que o worker de espelhamento do backend execute **imediatamente após a conclusão das transações no Lakehouse**, sem risco de dados incompletos e sem *blind delays*:

* **Módulo Implementado:** [`src/observability/webhook_notifier.py`](file:///c:/Users/marlo/Downloads/QIMED/src/observability/webhook_notifier.py)
* **Tarefa Airflow:** `notify_backend_mirror_trigger` (última task das DAGs `qimed_master_pipeline_end_to_end` e `qimed_gold_aggregation`).
* **Variável de Ambiente:** `BACKEND_SYNC_WEBHOOK_URL` (Default: `http://localhost:8000/api/v1/sync/mirror-trigger`).

#### 📦 Payload Enviado no POST para o Backend:
```json
{
  "event": "PIPELINE_COMPLETED",
  "dag_id": "qimed_master_pipeline_end_to_end",
  "dag_run_id": "scheduled__2026-05-01T00:00:00+00:00",
  "status": "success",
  "execution_id": "exec_1787161200_a1b2c3",
  "tables_ready": [
    "vw_internacoes_consolidadas",
    "dm_glosas_auditoria",
    "dm_hospital_efficiency",
    "dm_patient_readmissions",
    "aud_alertas_anomalias",
    "dm_ans_glosas_operadoras"
  ]
}
```

---

### 4.2. Como o Worker lê a Camada Silver (Delta Lake) para o Arquivo Morto

A **Camada Silver** reside fisicamente em diretórios **Delta Lake** (`lakehouse/silver/`):
* `lakehouse/silver/dim_operadoras_saude`
* `lakehouse/silver/fct_tiss_glosas_operadoras`

O worker lê **diretamente do Delta Lake** usando a biblioteca oficial **`deltalake` (Delta-RS)** e **`pyarrow.dataset`**, realizando streaming em lotes com *pushdown predicates* (filtrando apenas `dt_carga > watermark_atual`):

```mermaid
graph LR
    subgraph "QIMED Lakehouse Silver (Delta Lake)"
        D1["lakehouse/silver/dim_operadoras_saude<br/>(Delta Parquet)"]
        D2["lakehouse/silver/fct_tiss_glosas_operadoras<br/>(Delta Parquet Particionado)"]
    end

    subgraph "Worker de Espelhamento (Python / Delta-RS)"
        W1["DeltaTable.to_pyarrow_dataset()"]
        W2["Scanner Streaming (Batch 10.000)"]
    end

    subgraph "Banco da Aplicação (PostgreSQL)"
        P1["dim_operadoras_saude (UPSERT)"]
        P2["fct_tiss_glosas_operadoras (Bulk Append)"]
        P3["sys_sync_watermarks (Controle de Sync)"]
    end

    D1 --> W1 --> P1
    D2 --> W2 --> P2
    W2 --> P3
```

---

### 4.3. DDLs das Tabelas Silver no Banco da Aplicação (PostgreSQL)

Execute estes scripts no PostgreSQL para criar as tabelas de auditoria histórica e controle de sincronização:

```sql
-- 1. Tabela de Controle de Sincronização do Worker
CREATE TABLE IF NOT EXISTS sys_sync_watermarks (
    tabela_destino          VARCHAR(100) PRIMARY KEY,
    ultimo_watermark        TIMESTAMP NOT NULL,
    total_linhas_sync       BIGINT DEFAULT 0,
    ultima_execucao_em      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. Tabela Espelho da Dimensão Operadoras
CREATE TABLE IF NOT EXISTS dim_operadoras_saude (
    codigo_registro_ans     VARCHAR(10) PRIMARY KEY,
    cnpj_operadora          VARCHAR(18),
    razao_social            VARCHAR(255) NOT NULL,
    nome_fantasia           VARCHAR(255),
    modalidade_operadora    VARCHAR(50) NOT NULL,
    porte_operadora         VARCHAR(20) NOT NULL,
    uf_sede                 VARCHAR(2),
    situacao_operadora      VARCHAR(30) DEFAULT 'Ativa',
    dt_atualizacao          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. Tabela Espelho do Arquivo Morto / Fato Silver Transacional (Guias e Demonstrativos)
CREATE TABLE IF NOT EXISTS fct_tiss_glosas_operadoras (
    id_guia_tiss            VARCHAR(64) PRIMARY KEY, -- SHA-256 natural
    numero_guia_prestador   VARCHAR(50) NOT NULL,
    codigo_registro_ans     VARCHAR(10) NOT NULL REFERENCES dim_operadoras_saude(codigo_registro_ans),
    ano_competencia         INT NOT NULL,
    mes_competencia         INT NOT NULL,
    data_emissao_guia       DATE,
    data_pagamento_guia     DATE,
    tempo_processamento_dias INT,
    valor_informado_brl     NUMERIC(14,2) NOT NULL,
    valor_processado_brl    NUMERIC(14,2) NOT NULL,
    valor_glosa_inicial_brl NUMERIC(14,2) DEFAULT 0.00,
    valor_glosa_final_brl   NUMERIC(14,2) DEFAULT 0.00,
    status_retorno_guia     VARCHAR(30), -- 'Paga', 'Glosada Total', 'Sem Retorno >60d'
    dt_carga                TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_fct_tiss_operadora_periodo 
ON fct_tiss_glosas_operadoras (codigo_registro_ans, ano_competencia, mes_competencia);

CREATE INDEX IF NOT EXISTS idx_fct_tiss_dt_carga 
ON fct_tiss_glosas_operadoras (dt_carga);
```

---

### 4.4. Implementação Python do Worker de Espelhamento Delta $\rightarrow$ PostgreSQL

```python
import os
from datetime import datetime
from deltalake import DeltaTable
import pyarrow.dataset as ds
import psycopg2
from psycopg2.extras import execute_values

SILVER_PATH_DIM_OPERADORAS = "lakehouse/silver/dim_operadoras_saude"
SILVER_PATH_FCT_GLOSAS = "lakehouse/silver/fct_tiss_glosas_operadoras"
PG_DSN = os.getenv("DATABASE_URL", "postgresql://user:pass@localhost:5432/meu_app_db")

def sync_silver_delta_to_postgres():
    conn_pg = psycopg2.connect(PG_DSN)
    cur_pg = conn_pg.cursor()

    # 1. Recupera o último Watermark sincronizado
    cur_pg.execute("SELECT ultimo_watermark FROM sys_sync_watermarks WHERE tabela_destino = 'fct_tiss_glosas_operadoras'")
    row = cur_pg.fetchone()
    last_watermark = row[0] if row else datetime(1970, 1, 1)

    # 2. Espelhar dim_operadoras_saude (UPSERT lendo Delta Table)
    dt_operadoras = DeltaTable(SILVER_PATH_DIM_OPERADORAS)
    arrow_ops = dt_operadoras.to_pyarrow_table()
    ops_records = arrow_ops.to_pylist()

    upsert_dim_sql = """
        INSERT INTO dim_operadoras_saude (
            codigo_registro_ans, cnpj_operadora, razao_social,
            nome_fantasia, modalidade_operadora, porte_operadora, uf_sede
        ) VALUES (
            %(codigo_registro_ans)s, %(cnpj_operadora)s, %(razao_social)s,
            %(nome_fantasia)s, %(modalidade_operadora)s, %(porte_operadora)s, %(uf_sede)s
        )
        ON CONFLICT (codigo_registro_ans) DO UPDATE SET
            razao_social = EXCLUDED.razao_social,
            porte_operadora = EXCLUDED.porte_operadora,
            modalidade_operadora = EXCLUDED.modalidade_operadora,
            dt_atualizacao = CURRENT_TIMESTAMP;
    """
    execute_values(cur_pg, upsert_dim_sql, [
        (
            r["codigo_registro_ans"], r.get("cnpj_operadora"), r["razao_social"],
            r.get("nome_fantasia"), r["modalidade_operadora"], r["porte_operadora"], r.get("uf_sede")
        ) for r in ops_records
    ], page_size=1000)
    conn_pg.commit()

    # 3. Espelhar fct_tiss_glosas_operadoras com Scanner Incremental (Batch 10.000)
    dt_glosas = DeltaTable(SILVER_PATH_FCT_GLOSAS)
    arrow_dataset = dt_glosas.to_pyarrow_dataset()
    scanner = arrow_dataset.scanner(
        filter=(ds.field("dt_carga") > last_watermark),
        batch_size=10000
    )

    insert_fato_sql = """
        INSERT INTO fct_tiss_glosas_operadoras (
            id_guia_tiss, numero_guia_prestador, codigo_registro_ans,
            ano_competencia, mes_competencia, data_emissao_guia,
            data_pagamento_guia, tempo_processamento_dias, valor_informado_brl,
            valor_processado_brl, valor_glosa_inicial_brl, valor_glosa_final_brl,
            status_retorno_guia, dt_carga
        ) VALUES %s
        ON CONFLICT (id_guia_tiss) DO NOTHING;
    """

    total_inserido = 0
    novo_watermark = last_watermark

    for record_batch in scanner.to_batches():
        batch_list = record_batch.to_pylist()
        if not batch_list:
            continue

        rows = [
            (
                r["id_guia_tiss"], r["numero_guia_prestador"], r["codigo_registro_ans"],
                r["ano_competencia"], r["mes_competencia"], r.get("data_emissao_guia"),
                r.get("data_pagamento_guia"), r.get("tempo_processamento_dias"),
                r["valor_informado_brl"], r["valor_processado_brl"],
                r.get("valor_glosa_inicial_brl", 0.0), r.get("valor_glosa_final_brl", 0.0),
                r.get("status_retorno_guia"), r["dt_carga"]
            )
            for r in batch_list
        ]

        execute_values(cur_pg, insert_fato_sql, rows, page_size=5000)
        total_inserido += len(rows)
        batch_max_dt = max(r["dt_carga"] for r in batch_list)
        if batch_max_dt > novo_watermark:
            novo_watermark = batch_max_dt
        conn_pg.commit()

    # 4. Atualiza Tabela de Controle
    cur_pg.execute("""
        INSERT INTO sys_sync_watermarks (tabela_destino, ultimo_watermark, total_linhas_sync, ultima_execucao_em)
        VALUES ('fct_tiss_glosas_operadoras', %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (tabela_destino) DO UPDATE SET
            ultimo_watermark = EXCLUDED.ultimo_watermark,
            total_linhas_sync = sys_sync_watermarks.total_linhas_sync + EXCLUDED.total_linhas_sync,
            ultima_execucao_em = CURRENT_TIMESTAMP;
    """, (novo_watermark, total_inserido))
    conn_pg.commit()

    cur_pg.close()
    conn_pg.close()
    print(f"[WORKER] Sincronizacao Delta -> PostgreSQL concluida ({total_inserido:,} registros).")
```

---

## 🏥 5. Glosa de Operadoras ANS (TISS)

### 5.1. Origem e Legislação dos Indicadores
* **Base Normativa:** **Notas Técnicas nº 18/2020 e nº 25/2020/GEEIQ/DIRAD-DIDES/ANS** (Painel de Indicadores de Glosas e Prazos TISS).
* **Datasets de Origem:** Dados Abertos D-TISS (Demonstrativos de retorno de contas), CADOP (`Relatorio_cadop.csv`) e Tabela 38 da TUSS/ANS.

### 5.2. Granularidade dos Dados
* **Na Camada Silver (`fct_tiss_glosas_operadoras`):** Transacional por guia/item de demonstrativo.
* **Na Camada Gold (`dm_ans_glosas_operadoras`):** Agregada por:
  $$\textbf{Operadora} \times \textbf{Ano/Mês} \times \textbf{Porte} \times \textbf{Modalidade} \times \textbf{Segmentação}$$

### 5.3. Disponibilidade Temporal
* **Histórico:** Disponível de **2015 até 2026**.
* **Periodicidade:** Mensal (competências `AAAAMM`).

---

## 💾 6. DDL Oficial da Tabela Espelho no Banco da Aplicação

Execute este script no banco de dados operacional da aplicação (PostgreSQL/MySQL):

```sql
CREATE TABLE IF NOT EXISTS dm_ans_glosas_operadoras (
    -- Chave Primária / Surrogate Key
    id_registro_kpi                 VARCHAR(64) PRIMARY KEY,
    
    -- Identificação da Operadora (Dimensão Embutida)
    codigo_registro_ans             VARCHAR(10) NOT NULL,
    cnpj_operadora                  VARCHAR(18),
    razao_social                    VARCHAR(255) NOT NULL,
    operadora_label                 VARCHAR(300) NOT NULL, -- '005711 - BRADESCO SAÚDE S.A.'
    
    -- Dimensões Canônicas da ANS
    porte_operadora                 VARCHAR(20) NOT NULL,  -- 'Pequeno', 'Medio', 'Grande'
    modalidade_operadora            VARCHAR(50) NOT NULL,  -- 'Cooperativa Medica', 'Medicina de Grupo', etc.
    segmentacao_assistencial        VARCHAR(50) NOT NULL,  -- 'Ambulatorial', 'Hospitalar c/ Obstetricia', etc.
    
    -- Janela Temporal
    ano                             VARCHAR(4) NOT NULL,
    mes                             VARCHAR(2) NOT NULL,
    periodo                         VARCHAR(7) NOT NULL,   -- '2026-05'
    trimestre                       VARCHAR(2),            -- 'Q2'
    
    -- 5 Indicadores do Setor (Com 2 casas decimais)
    tempo_medio_pagamento_dias      DOUBLE PRECISION DEFAULT 0.0,
    taxa_glosa_inicial_pct          DOUBLE PRECISION DEFAULT 0.0,
    taxa_glosa_final_pct            DOUBLE PRECISION DEFAULT 0.0,
    pct_guias_sem_retorno_60d       DOUBLE PRECISION DEFAULT 0.0,
    pct_valor_guias_sem_retorno_60d DOUBLE PRECISION DEFAULT 0.0,
    
    -- Numeradores e Denominadores
    total_faturado_brl              DOUBLE PRECISION DEFAULT 0.0,
    total_glosado_inicial_brl       DOUBLE PRECISION DEFAULT 0.0,
    total_glosado_final_brl         DOUBLE PRECISION DEFAULT 0.0,
    qtd_guias_total                 BIGINT DEFAULT 0,
    qtd_guias_sem_retorno_60d       BIGINT DEFAULT 0,
    valor_guias_sem_retorno_60d     DOUBLE PRECISION DEFAULT 0.0,
    
    -- Card de Operadora Atípica
    is_operadora_atipica            BOOLEAN DEFAULT FALSE,
    motivo_atipicidade              VARCHAR(255),
    
    -- Controle de Sincronização Incremental
    dt_carga                        TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    id_execucao                     VARCHAR(64) NOT NULL
);

-- Índices de Performance e Upsert
CREATE UNIQUE INDEX IF NOT EXISTS uk_ans_glosas_operadora_periodo 
ON dm_ans_glosas_operadoras (codigo_registro_ans, ano, mes, modalidade_operadora, segmentacao_assistencial);

CREATE INDEX IF NOT EXISTS idx_ans_glosas_dt_carga ON dm_ans_glosas_operadoras (dt_carga);
CREATE INDEX IF NOT EXISTS idx_ans_glosas_porte ON dm_ans_glosas_operadoras (porte_operadora);
```

---

## 🎯 7. Detecção de Outlier e Média Setorial Ajustada (Regra de Negócio)

### 7.1. Regra Operacional do Frontend
1. **Detectar:** Identificar operadoras que concentram $\ge 90\%$ da glosa final no subconjunto filtrado.
2. **Alertar:** Exibir o **Card de Operadora Atípica** na interface com nome, valor e percentual de concentração.
3. **Ajustar:** Excluir a operadora atípica do cálculo da média setorial dos cards de topo (**Trimmed Mean**), evitando distorções.
4. **Preservar:** Manter a operadora atípica 100% listada e consultável na tabela detalhada para auditoria.

### 7.2. Implementação no Backend (Python)
```python
def calcular_kpis_setoriais_ajustados(operadoras_list: list, limiar_outlier: float = 0.90):
    total_glosa_setor = sum(op["total_glosado_final_brl"] for op in operadoras_list)
    total_faturado_setor = sum(op["total_faturado_brl"] for op in operadoras_list)
    
    outlier_detectado = None
    operadoras_para_media = operadoras_list
    
    if total_glosa_setor > 0:
        for op in operadoras_list:
            share = op["total_glosado_final_brl"] / total_glosa_setor
            if share >= limiar_outlier:
                outlier_detectado = {
                    "operadora_label": op["operadora_label"],
                    "share_glosa_pct": round(share * 100.0, 2),
                    "valor_glosado_brl": op["total_glosado_final_brl"],
                    "mensagem": f"A operadora {op['operadora_label']} concentra {share*100:.1f}% de toda a glosa final do setor."
                }
                # Exclui da média de referência setorial
                operadoras_para_media = [o for o in operadoras_list if o["codigo_registro_ans"] != op["codigo_registro_ans"]]
                break

    # Recalcula média setorial ajustada
    fat_val = sum(o["total_faturado_brl"] for o in operadoras_para_media)
    glo_val = sum(o["total_glosado_final_brl"] for o in operadoras_para_media)
    taxa_glosa_ajustada = (glo_val / fat_val * 100.0) if fat_val > 0 else 0.0

    return {
        "kpis_setoriais": {
            "taxa_glosa_final_pct": round(taxa_glosa_ajustada, 2),
            "tempo_medio_pagamento_dias": round(
                sum(o["tempo_medio_pagamento_dias"] for o in operadoras_para_media) / max(1, len(operadoras_para_media)), 1
            ),
        },
        "card_operadora_atipica": outlier_detectado,
        "operadoras_detalhe": operadoras_list # Lista completa preservada
    }
```

---

## 🗂️ 8. Configuração Detalhada de Todas as DAGs do Airflow

Abaixo está a matriz completa de configuração, identificadores, agendamentos, tarefas e parâmetros de cada uma das **12 DAGs** registradas no Apache Airflow do QIMED:

```mermaid
graph TD
    subgraph "DAGs de Ingestão e Coletores Granulares (Camada Bronze)"
        D1["qimed_datasus_sih<br/>(Internações SUS RD)"]
        D2["qimed_datasus_cnes<br/>(Estabelecimentos ST)"]
        D3["qimed_datasus_sia<br/>(Ambulatório PA)"]
        D4["qimed_datasus_sih_rejeicoes_glosas<br/>(Rejeições RJ e Críticas ER)"]
        D5["qimed_ans_supplementary_health<br/>(Operadoras, Beneficiários, ABI, NIP)"]
        D6["qimed_fhir_synthetic<br/>(Microdados Clínicos Sintéticos)"]
        D7["qimed_sisreg_regulation<br/>(Filas e Encaminhamentos)"]
        D8["qimed_datasus_epidemiology_aps<br/>(Vigilância Epidemiológica e APS)"]
    end

    subgraph "DAGs de Transformação e Governança"
        T1["qimed_dim_tempo_generator<br/>(Dimensão Calendário)"]
        T2["qimed_silver_transformation<br/>(MPI, Canonização, IBGE & CID-10)"]
        G1["qimed_gold_aggregation<br/>(Data Marts e DW DuckDB)"]
        Q1["qimed_data_quality_audit<br/>(Auditoria Forense Automatizada)"]
    end

    subgraph "DAG Master Fim a Fim"
        M1["qimed_master_pipeline_end_to_end<br/>(Ingestão 27 UFs >> Silver >> Gold DW)"]
    end

    D1 & D2 & D3 & D4 & D5 & D6 & D7 & D8 --> T2 --> G1 --> Q1
    T1 --> T2
    M1 -.->|Orquestra em Cascata| D1 & T2 & G1 & Q1
```

### 📋 Matriz de Configuração das DAGs:

| # | DAG ID | Schedule | Retries | Descrição Semântica / Escopo | Tarefas Encadeadas (Task Flow) |
|---|---|:---:|:---:|---|---|
| **1** | **`qimed_master_pipeline_end_to_end`** | `@monthly` | 2 (3 min) | **Pipeline Master:** Orquestra download das 27 UFs, transformação Silver e carga Gold no DW. | `ingest_datasus_bronze_27_ufs` $\rightarrow$ `transform_silver_lakehouse` $\rightarrow$ `aggregate_gold_data_marts_dw` |
| **2** | **`qimed_datasus_sih`** | `@monthly` | 2 (2 min) | **SIH/SUS:** Ingestão de AIHs Reduzidas (`RD*.dbc`), anonimização LGPD e gravação Delta Bronze. | `download_and_ingest_sih_27_ufs` |
| **3** | **`qimed_datasus_sih_rejeicoes_glosas`** | `@monthly` | 3 (5 min) | **SIH Glosas:** Ingestão de AIHs rejeitadas (`RJ*.dbc`) e críticas (`ER*.dbc`) $\rightarrow$ `fct_glosas_hospitalares`. | `ingest_sih_rejeicoes_bronze` $\rightarrow$ `transform_fct_glosas_silver` $\rightarrow$ `catalog_glosas_dataset` |
| **4** | **`qimed_datasus_cnes`** | `@monthly` | 3 (5 min) | **CNES:** Coleta de estabelecimentos, leitos e mantenedoras (`ST*.dbc`) com catalogação Delta. | `download_cnes` $\rightarrow$ `validate_cnes` $\rightarrow$ `anonymize_cnes` $\rightarrow$ `write_bronze_cnes` $\rightarrow$ `catalog_cnes` |
| **5** | **`qimed_datasus_sia`** | `@monthly` | 2 (2 min) | **SIA/SUS:** Produção ambulatorial (`PA*.dbc`) com auto-discovery multipart e streaming Arrow. | `download_and_ingest_sia_27_ufs` |
| **6** | **`qimed_ans_supplementary_health`** | `@monthly` | 3 (5 min) | **ANS:** Ingestão de Cadop, Beneficiários SIB, Ressarcimento ABI e Notificações NIP. | `run_ans_ingestion_pipeline` $\rightarrow$ `catalog_ans_dataset` |
| **7** | **`qimed_fhir_synthetic`** | `@monthly` | 3 (5 min) | **FHIR R4:** Geração sintética de pacientes, encontros, condições e procedimentos em bundles JSON. | `generate_fhir_bundle` $\rightarrow$ `validate_fhir` $\rightarrow$ `anonymize_fhir` $\rightarrow$ `write_bronze_fhir` $\rightarrow$ `catalog_fhir` |
| **8** | **`qimed_sisreg_regulation`** | `@monthly` | 2 (5 min) | **SISREG:** Ingestão de filas regulatórias, solicitações de exames e tempos de espera ambulatorial. | `ingest_sisreg_regulation_data` |
| **9** | **`qimed_datasus_epidemiology_aps`** | `@monthly` | 2 (5 min) | **APS & SINAN:** Coleta de dados da Atenção Primária (SISAB) e agravos de notificação (SINAN). | `ingest_epidemiology_and_aps_data` |
| **10** | **`qimed_dim_tempo_generator`** | `@monthly` | 2 (2 min) | **Dimensão Calendário:** Geração e atualização contínua de datas, dias úteis, trimestres e semestres. | `generate_dim_tempo_delta` $\rightarrow$ `catalog_dim_tempo` |
| **11** | **`qimed_silver_transformation`** | `@monthly` | 2 (3 min) | **Silver Master:** Resolução de entidades (MPI em 4 níveis), normalização de CIDs e municípios. | `run_canonical_silver_transformations` |
| **12** | **`qimed_gold_aggregation`** | `@monthly` | 2 (3 min) | **Gold DW:** Construção de todos os Data Marts analíticos, views semânticas e Central de Anomalias. | `build_gold_data_marts_and_views` |
| **13** | **`qimed_data_quality_audit`** | `@daily` | 2 (3 min) | **Data Quality:** Auditoria forense automatizada de completude, anomalias e conformidade do DW. | `audit_warehouse_data_quality` |

---

### ⚙️ Exemplo de Parâmetros Aceitos via Disparo REST API:

Quando o backend aciona qualquer uma dessas DAGs via `POST /api/v1/dags/{dag_id}/dagRuns`, ele pode injetar parâmetros no objeto `"conf"`:

```json
{
  "conf": {
    "uf": "CE",                // UF desejada ou "BR" para todas as 27 UFs
    "year": 2026,              // Ano de competência
    "month": 5,                // Mês de competência (1 a 12)
    "modalidade": "operadoras",// Exclusivo ANS: "operadoras", "beneficiarios", "ressarcimento", "nip"
    "force_reprocess": false   // Se true, sobrescreve caches locais
  }
}
```

---

## 💻 9. Configuração Mínima de Infraestrutura, Ambiente & Parametrização

### 9.1. Requisitos de Hardware do Servidor / Máquina

| Recurso | Mínimo (Modo Script Python) | Recomendado (Modo Docker + Airflow) |
|---|:---:|:---:|
| **Processador (CPU)** | **2 Núcleos / vCPUs** (x86_64 ou ARM64) | **4 Núcleos / vCPUs** |
| **Memória RAM** | **4 GB** (Pico de uso: ~1.2 GB no DuckDB) | **8 GB** (Airflow + PostgreSQL + DuckDB) |
| **Armazenamento (Disco)** | **10 GB livres (SSD)** | **20 GB livres (SSD)** |
| **Rede** | Acesso de saída HTTP/FTP (`ftp.datasus.gov.br`) | Conexão banda larga estável |
| **Sistema Operacional** | Linux (Ubuntu/Debian), Windows 10/11 ou macOS | Linux, Windows (com WSL2) ou macOS |

---

### 9.2. Variáveis de Ambiente Essenciais (`.env`)

| Variável | Obrigatória? | Padrão / Exemplo | Finalidade |
|---|:---:|---|---|
| `QIMED_MPI_SALT` | **SIM** | `openssl rand -hex 32` | Salt criptográfico HMAC SHA-256 para anonimização e MPI de pacientes. O pipeline recusa execução se ausente. |
| `SALT_SECRET` | **SIM** | `openssl rand -hex 32` | Chave de reforço da camada de pseudoanonimização LGPD. |
| `BACKEND_SYNC_WEBHOOK_URL` | NÃO | `http://localhost:8000/api/v1/sync/mirror-trigger` | Endpoint HTTP POST notificado ao término bem-sucedido das DAGs para disparar o espelhamento. |
| `LAKEHOUSE_PATH` | NÃO | `lakehouse/bronze` | Caminho raiz da Camada Bronze. |
| `LAKEHOUSE_ROOT` | NÃO | `lakehouse` | Raiz de persistência das camadas Bronze e Silver do Delta Lake. |
| `WAREHOUSE_PATH` | NÃO | `warehouse` | Diretório onde o arquivo OLAP `qimed_dw.duckdb` é gerado. |
| `DATASUS_FTP_HOST` | NÃO | `ftp.datasus.gov.br` | Host oficial do servidor FTP do DATASUS. |
| `LOG_LEVEL` | NÃO | `INFO` | Nível de verbosidade de logs (`DEBUG`, `INFO`, `WARNING`, `ERROR`). |
| `AIRFLOW__WEBSERVER__SECRET_KEY` | NÃO | `qimed_secret_key` | Chave de sessão do painel web do Apache Airflow. |

---

### 9.3. Regras de Parametrização por Mês, Ano e UF

1. **Janelas de Competência Válidas:**
   * **DATASUS (SIH e SIA):** Os arquivos `RD*.dbc` e `PA*.dbc` são publicados mensalmente com **1 a 2 meses de defasagem** (ex.: dados de Maio/2026 ficam disponíveis entre Junho e Julho/2026).
   * **Histórico Disponível:** O pipeline suporta processamento retroativo de **2015 até o ano corrente**.
2. **Definição de UF / Estado:**
   * O pipeline aceita qualquer uma das **27 Unidades Federativas** (`RO`, `AC`, `AM`, `RR`, `PA`, `AP`, `TO`, `MA`, `PI`, `CE`, `RN`, `PB`, `PE`, `AL`, `SE`, `BA`, `MG`, `ES`, `RJ`, `SP`, `PR`, `SC`, `RS`, `MS`, `MT`, `GO`, `DF`) ou `"BR"` para processar o país inteiro em paralelo.
3. **Carga Incremental vs Reprocessamento Forçado:**
   * Por padrão (`force_reprocess=False`), o pipeline consulta o manifesto Delta (`_metadata/delta_manifest`) e **pula partições já commitadas com sucesso**, economizando banda e tempo.
   * Para forçar o re-download e sobrescrita da partição: envie `"force_reprocess": true` no payload da API ou `--force` na linha de comando.


