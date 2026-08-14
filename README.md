# QIMED DataQore — Pipeline de Dados e Arquitetura Lakehouse

Plataforma de engenharia de dados e interoperabilidade em saude digital desenvolvida para ingestao, validacao, anonimizacao e normalizacao semantica de microdados publicos (DATASUS) e mensagens clinicas estruturadas (HL7 FHIR R4). O ecossistema consolida os dados em uma arquitetura Lakehouse baseada em Delta Lake (camadas Bronze e Silver) para alimentar modelos analiticos, AutoML e otimizadores operacionais.

---

## 1. Arquitetura do Sistema

O pipeline segue o padrao de camadas Lakehouse com processamento desacoplado entre computacao e armazenamento:

```
[Fontes de Dados]
  ├── DATASUS FTP (SIH - Internacoes / CNES - Estabelecimentos)
  └── Mensageria Clinica (HL7 FHIR R4 Bundles)
          │
          ▼
[Camada de Ingestao & LGPD Gate]
  ├── Coletores com retry, checkpoint e circuit breaker
  ├── Descompressao binaria proprietaria (DBC para DBF)
  ├── Identificacao declarativa de PII (pii_manifest.yaml)
  └── Pseudoanonimizacao deterministica (SHA-256 + Salt rotativo)
          │
          ▼
[Camada Bronze - Delta Lake]
  ├── Dados brutos/semiestruturados com metadados de ingestao
  └── Particionamento temporal (ano/mes) com deduplicacao por hash
          │
          ▼
[Camada Silver - Normalizacao Semantica & Entidades Canonicas]
  ├── Servico de Terminologias (CID-10, SIGTAP, IBGE Municipios, CBO)
  ├── Mapeadores semanticos (SIH/CNES/FHIR para modelo canonico)
  ├── Resolucao de Identidades (Master Patient Index - MPI / Master Facility Index - MFI)
  └── Tabelas Delta normalizadas (dim_patients, dim_organizations, fct_encounters, fct_conditions, fct_procedures)
          │
          ▼
[Orquestracao & Consumo]
  ├── DAGs Apache Airflow (LocalExecutor + PostgreSQL)
  └── Catalogo de Metadados e Linhagem (JSON/Schema registry)
```

---

## 2. Componentes Implementados

### 2.1. Coletores de Dados (`src/collectors/`)
- **`BaseCollector`**: Classe abstrata que padroniza o ciclo de vida da ingestao (`fetch` -> `parse` -> `detect_pii` -> `anonymize` -> `validate` -> `write_bronze` -> `register_catalog`). Implementa controle de estado com checkpoints em disco, politicas de retry com backoff exponencial e circuit breaker para proteger servicos externos.
- **`DatasusCollector`**: Integracao direta com os servidores FTP publicos do DATASUS (`ftp.datasus.gov.br`). Realiza download de arquivos `RD*.dbc` (SIH) e `ST*.dbc` (CNES), descompressao binaria com `pyreaddbc` e extracao de DataFrames via `dbfread`.
- **`FhirSyntheticCollector`**: Gerador e extrator de Bundles FHIR R4 sinteticos contendo recursos completos de interoperabilidade clinica.

### 2.2. Simulador e Gerador de Dados Sinteticos FHIR R4
Para viabilizar o desenvolvimento, testes de carga, validacao de esquemas e demonstracoes sem dependencia de conexoes diretas com prontuarios eletronicos (PEP) ou servidores FHIR externos em fase inicial de MVP, o projeto inclui um modulo gerador de dados clinicos sinteticos:
- **Estrutura de Bundles FHIR R4**: Gera colecoes completas de recursos interligados por referencias semanticas (`Patient/id`, `Encounter/id`, `Organization/id`).
- **Recursos Gerados**:
  - `Patient`: Dados demograficos com nomes brasileiros realistas, datas de nascimento coerentes, genero e identificadores no padrao CPF/RNDS.
  - `Organization`: Estabelecimentos hospitalares vinculados a codigos CNES validos.
  - `Encounter`: Atendimentos e internacoes com periodos de permanencia, classes de atendimento (`IMP` - internacao, `EMER` - urgencia, `AMB` - ambulatorial) e vinculo com o prestador de servico.
  - `Condition`: Diagnosticos primarios e secundarios mapeados na codificacao oficial CID-10 (`http://hl7.org/fhir/sid/icd-10`).
  - `Observation`: Sinais vitais e metricas clinicas (ex: frequencia cardiaca) codificados em padrao LOINC.
  - `Procedure`: Procedimentos terapeuticos e cirurgicos associados a tabela SIGTAP do SUS (`http://sigtap.datasus.gov.br`).
- **Parsing Tabular**: O coletor achata os recursos semiestruturados do JSON/Bundle em formato tabular padronizado, preservando o JSON bruto para auditoria e viabilizando a passagem direta pelo LGPD Gate.

### 2.3. Validadores de Qualidade (`src/validators/`)
- **`DatasusValidator`**: Validacao estrutural e de schema para registros de AIH (presenca de colunas obrigatorias, tipos de dados, consistencia de codigos de procedimento e diagnostico).
- **`FhirValidator`**: Validacao de recursos FHIR, verificando integridade de identificadores, tipos de recursos suportados e formato de datas ISO-8601.

### 2.4. Seguranca e Conformidade LGPD (`src/lgpd/`)
- **`PIIDetector`**: Mapeamento declarativo via `config/pii_manifest.yaml` para deteccao automatica de colunas contendo dados pessoais (`NASC`, `N_AIH`, `CPF_AUT`, `CPF_PROF`, `name_family`, `name_given`, `cpf`, etc.).
- **`Anonymizer`**: Algoritmo de hash criptografico unidirecional SHA-256 com adicao de salt configuravel (`SALT_SECRET`). Permite consistencia relacional (linkage de registros do mesmo paciente) sem expor identificadores em texto claro na camada Bronze ou Silver.

### 2.5. Servico de Terminologias (`src/silver/terminology.py`)
- **CID-10**: Validacao de codigos diagnosticos, normalizacao de subcategorias e classificacao automatica nos 21 capitulos clinicos da OMS.
- **SIGTAP (SUS)**: Formatacao de codigos de procedimento de 10 digitos (`GG.SS.FF.NNN-D`) e associacao com os grupos oficiais de atendimento do SUS.
- **IBGE**: Mapeamento de codigos municipais de 6 e 7 digitos com identificacao da Unidade Federativa correspondente.

### 2.6. Mapeadores Semanticos & Resolucao de Entidades (`src/silver/`)
- **Mapeadores Canonicos (`mappers/`)**: Conversores dedicados para transformar microdados do SIH, CNES e Bundles FHIR em entidades dimensionais e fatos alinhadas ao padrao canocico.
- **`EntityResolver`**: Motor de resolucao de identidades mestras (Master Patient Index) gerando chaves unificadas (`patient_master_id` no formato `mpi_*`) para rastreamento longitudinal da jornada do paciente entre multiplos estabelecimentos.

### 2.7. Persistencia Lakehouse (`src/lakehouse/`)
- **`BronzeWriter`**: Escrita em tabelas Delta Lake com particionamento por ano/mes, inclusao de campos de auditoria (`_ingested_at`, `_source_type`, `_source_file`) e controle de append.
- **`SilverWriter`**: Escrita atômica das tabelas dimensionais e de fatos com suporte a evolucao de schema (`schema_mode="merge"`).
- **`DeltaManager`**: Manutencao de tabelas Delta, consultas de time-travel e compactacao de pequenos arquivos.

---

## 3. Estrutura de Diretorios

```
QIMED/
├── config/
│   ├── pii_manifest.yaml          # Mapeamento de campos sensiveis por fonte
│   └── sources.yaml               # Parametros das fontes de dados
├── dags/
│   ├── dag_datasus_cnes.py        # Ingestao mensal do CNES (DATASUS FTP)
│   ├── dag_datasus_sih.py         # Ingestao mensal do SIH (DATASUS FTP)
│   ├── dag_fhir_synthetic.py      # Geracao e ingestao de dados FHIR
│   └── dag_silver_transformation.py # Transformacao semantica Bronze -> Silver
├── scripts/
│   ├── run_fhir_ingestion.py      # Execucao standalone da ingestao FHIR
│   ├── run_silver_pipeline.py     # Execucao standalone da Camada Silver
│   ├── test_datasus_live_extraction.py # Teste de extracao real DATASUS FTP
│   ├── verify_bronze.py           # Inspecao e validacao da Camada Bronze
│   └── verify_silver.py           # Inspecao e validacao da Camada Silver
├── src/
│   ├── collectors/                # Modulos de extracao, descompressao e geracao FHIR
│   ├── lakehouse/                 # Escritores e gerenciadores Delta Lake
│   ├── lgpd/                      # Deteccao e anonimizacao de PII
│   ├── metadata/                  # Catalogo de datasets ingeridos
│   ├── silver/                    # Terminologias, mappers e entity resolution
│   ├── utils/                     # Configuracao de logs estruturados em JSON
│   └── validators/                # Validadores de schema e qualidade
├── tests/                         # Suite de testes unitarios e integrados
├── docker-compose.yml             # Orquestracao do Airflow e PostgreSQL
├── Dockerfile                     # Imagem Docker customizada
├── requirements.txt               # Dependencias Python fixadas
└── README.md                      # Documentacao tecnica do projeto
```

---

## 4. Requisitos e Instalacao

### Pre-requisitos
- Python 3.11+
- Docker Engine e Docker Compose (para orquestracao via Airflow)

### Instalacao do Ambiente Local

1. Clone o repositorio:
```bash
git clone https://github.com/marlonmelo12/QIMED_Pipeline.git
cd QIMED_Pipeline
```

2. Crie e ative um ambiente virtual:
```bash
python -m venv venv
# Linux / macOS:
source venv/bin/activate
# Windows:
.\venv\Scripts\activate
```

3. Instale as dependencias:
```bash
pip install -r requirements.txt
```

4. Configure as variaveis de ambiente:
```bash
cp .env.example .env
```

---

## 5. Execucao e Validacao

### 5.1. Executar Testes Automatizados
A suite cobre testes unitarios de anonimizacao, validadores, gravacao Delta Lake, gerador FHIR, terminologias e resolucao de identidades:

```bash
python -m pytest tests/ -v
```

### 5.2. Executar Pipelines Standalone

- **Geracao e Ingestao de dados clinicos FHIR (Bronze)**:
```bash
python scripts/run_fhir_ingestion.py
```

- **Extracao e ingestao real do DATASUS FTP (Bronze + Silver)**:
```bash
python scripts/test_datasus_live_extraction.py
```

- **Processamento da Camada Silver (Normalizacao + MPI)**:
```bash
python scripts/run_silver_pipeline.py
```

- **Inspecao dos dados e schemas da Camada Silver**:
```bash
python scripts/verify_silver.py
```

### 5.3. Orquestracao via Apache Airflow

Suba o cluster local:
```bash
docker compose up -d
```

Acesse a interface web do Airflow em `http://localhost:8080` (credenciais padrao: `admin` / `admin`).
As seguintes DAGs estarao disponiveis para execucao agendada ou sob demanda:
- `qimed_datasus_sih`
- `qimed_datasus_cnes`
- `qimed_fhir_synthetic`
- `qimed_silver_transformation`

---

## 6. Modelo de Dados Silver

As tabelas geradas no Delta Lake seguem a seguinte modelagem:

- **`dim_patients`**: Entidade paciente unificada com `patient_master_id` (MPI), dados demograficos normalizados e hash anonimizado de nascimento.
- **`dim_organizations`**: Cadastro de hospitais e unidades com codigo CNES, tipo de estabelecimento, capacidade de leitos e localizacao.
- **`fct_encounters`**: Fato de internacoes e atendimentos contendo datas de admissao/alta, tempo de permanencia, classe do atendimento, desfecho e custo total.
- **`fct_conditions`**: Fato de diagnosticos associados a internacao, categorizados pelo codigo CID-10 e descricao do capitulo correspondente.
- **`fct_procedures`**: Fato de procedimentos realizados, com codigo SIGTAP formatado e classificacao por grupo de procedimento do SUS.
