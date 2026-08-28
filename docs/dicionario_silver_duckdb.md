# Dicionário de Dados — QIMED Lakehouse & DW V3

---

## `fct_internacao` [STATUS: ENTREGUE / SILVER ATIVA]

* **Origem:** DATASUS / SIH (Sistema de Informações Hospitalares - Tipo RD).
* **Granularidade:** 1 registro por AIH única consolidada (faturamentos iniciais tipo `1` e mensais de continuidade tipo `5`).

| Coluna | Tipo SQL | Chave / Restrição | Descrição |
|---|---|---|---|
| `id_internacao_hospitalar` | `VARCHAR` | PK Única | Surrogate Key determinística a nível de linha (Hash MD5). |
| `id_episodio_internacao` | `VARCHAR` | FK / Cluster | Chave de episódio contínuo que vincula AIHs 1 e 5 do mesmo atendimento. |
| `numero_aih` | `VARCHAR` | FK | Número da Autorização de Internação Hospitalar (`N_AIH`). |
| `numero_documento_autorizacao` | `VARCHAR` | Nullable | Número do laudo/documento de autorização de internação (`NUM_DOC` / `N_DOC`). |
| `sequencial_aih5` | `INTEGER` | Nullable | Número de ordem do faturamento continuado da AIH (`SEQ_AIH5`). |
| `tipo_identificacao_aih` | `VARCHAR` | — | Tipo de identificação da AIH (`1` = Inicial, `5` = Continuidade). |
| `motivo_saida` | `VARCHAR` | Nullable | Código do motivo de encerramento/alta do faturamento (`MOTSAID`). |
| `numero_aih_proxima` | `VARCHAR` | Nullable | Número da AIH subsequente encadeada (`AIH_PROX`). |
| `codigo_estabelecimento_cnes` | `VARCHAR` | FK | Código CNES do hospital prestador (`CNES`). |
| `codigo_municipio_residencia_paciente` | `VARCHAR` | FK | Código IBGE do município de residência do paciente (`MUNIC_RES`). |
| `uf_residencia_paciente` | `VARCHAR` | — | UF de residência derivada do código municipal do IBGE. |
| `codigo_municipio_hospital` | `VARCHAR` | FK | Código IBGE do município do hospital (`MUNIC_MOV`). |
| `data_nascimento_paciente` | `VARCHAR` | — | Data de nascimento no formato AAAAMMDD (`NASC`). |
| `sexo_biologico` | `VARCHAR` | — | Sexo biológico harmonizado: `'M'`, `'F'` ou `'I'` (`SEXO`). |
| `data_internacao` | `VARCHAR` | — | Data de admissão no leito hospitalar AAAAMMDD (`DT_INTER`). |
| `data_alta` | `VARCHAR` | — | Data de desfecho/alta hospitalar AAAAMMDD (`DT_SAIDA`). |
| `codigo_cid10_principal` | `VARCHAR` | FK | Diagnóstico principal causador da internação (`DIAG_PRINC`). |
| `codigo_cid10_secundario` | `VARCHAR` | FK / Nullable | Diagnóstico secundário informado (`DIAG_SECUN`). |
| `dias_permanencia_real` | `INTEGER` | — | Total de dias de ocupação de leito faturados no período (`DIAS_PERM`). |
| `dias_permanencia_faturados_mes` | `INTEGER` | — | Dias de permanência faturados na competência contábil mensal. |
| `dias_duracao_acumulada_episodio` | `BIGINT` | — | Duração total acumulada calculada em dias entre internação e alta. |
| `indicador_obito` | `BOOLEAN` | — | Indicador booleano de óbito hospitalar (`MORTE = '1'`). |
| `valor_total_brl` | `DOUBLE` | — | Valor total da AIH pago pelo SUS (`VAL_TOT`). |
| `valor_uti_brl` | `DOUBLE` | — | Valor pago por diárias de UTI (`VAL_UTI`). |
| `valor_servicos_hospitalares_brl` | `DOUBLE` | — | Valor pago por serviços hospitalares (`VAL_SH`). |
| `valor_servicos_profissionais_brl` | `DOUBLE` | — | Valor pago por serviços profissionais/médicos (`VAL_SP`). |
| `codigo_procedimento_realizado` | `VARCHAR` | FK | Código SIGTAP do procedimento realizado (`PROC_REA`). |
| `codigo_procedimento_solicitado` | `VARCHAR` | FK | Código SIGTAP do procedimento inicialmente solicitado (`PROC_SOLIC`). |
| `ano` | `VARCHAR` | Partição | Ano de competência da internação. |
| `mes` | `VARCHAR` | Partição | Mês de competência da internação. |
| `uf` | `VARCHAR` | Partição | Estado federativo da internação. |
| `id_execucao` | `VARCHAR` | — | Identificador de auditoria da execução do pipeline. |
| `identificador_atendimento` | `VARCHAR` | MPI Nível 1 | Identificador unívoco do atendimento (`N_AIH`). |
| `identificador_registro` | `VARCHAR` | MPI Nível 2 | Hash MD5 determinístico de auditoria da linha. |
| `pseudonimo_paciente` | `VARCHAR` | FK / MPI Nível 3 | Hash criptográfico irreversível do paciente. |
| `identificador_paciente_candidato` | `VARCHAR` | MPI Nível 4 | Cluster de vinculação probabilística longitudinal. |

---

## `fct_atendimentos_ambulatoriais` [STATUS: ENTREGUE / SILVER ATIVA]

* **Origem:** DATASUS / SIA (Sistema de Informações Ambulatoriais - Tipo PA/BPA).
* **Granularidade:** 1 registro por linha de faturamento ambulatorial faturado (BPA-Consolidado e Individualizado).

| Coluna | Tipo SQL | Chave / Restrição | Descrição |
|---|---|---|---|
| `id_atendimento_ambulatorial` | `VARCHAR` | PK Única | Surrogate Key determinística composta (Hash MD5). |
| `codigo_estabelecimento_cnes` | `VARCHAR` | FK | Código CNES da unidade executante (`PA_CODUNI`). |
| `codigo_gestor` | `VARCHAR` | FK | Código IBGE do gestor responsável pela conta (`PA_GESTAO`). |
| `codigo_municipio_estabelecimento` | `VARCHAR` | FK | Município onde o procedimento foi executado (`PA_UFMUN`). |
| `codigo_procedimento_sigtap` | `VARCHAR` | FK | Código SIGTAP do exame/consulta/terapia (`PA_PROC_ID`). |
| `codigo_cid10_principal` | `VARCHAR` | FK / Nullable | CID-10 principal da guia ambulatorial (`PA_CIDPRI`). |
| `codigo_cid10_secundario` | `VARCHAR` | FK / Nullable | CID-10 secundário informado (`PA_CIDSEC`). |
| `sexo_biologico` | `VARCHAR` | — | Sexo biológico harmonizado: `'M'`, `'F'` ou `'I'` (`PA_SEXO`). |
| `idade_paciente_anos` | `INTEGER` | Nullable | Idade do paciente em anos (`PA_IDADE`). |
| `codigo_municipio_residencia_paciente` | `VARCHAR` | FK | Município de residência do paciente (`PA_MUNPCN`). |
| `uf_residencia_paciente` | `VARCHAR` | — | UF de residência do paciente. |
| `quantidade_produzida` | `BIGINT` | — | Quantidade de procedimentos informados (`PA_QTDPRO`). |
| `quantidade_aprovada` | `BIGINT` | — | Quantidade de procedimentos homologados (`PA_QTDAPR`). |
| `valor_produzido_brl` | `DOUBLE` | — | Valor total cobrado na Tabela SUS (`PA_VALPRO`). |
| `valor_aprovado_brl` | `DOUBLE` | — | Valor líquido pago pelo SUS (`PA_VALAPR`). |
| `ano` | `VARCHAR` | Partição | Ano de competência da produção ambulatorial. |
| `mes` | `VARCHAR` | Partição | Mês de competência da produção ambulatorial. |
| `uf` | `VARCHAR` | Partição | Estado federativo de execução do procedimento. |
| `id_execucao` | `VARCHAR` | — | Identificador de auditoria da execução do pipeline. |

---

## `fct_ressarcimento_sus` [STATUS: ENTREGUE / SILVER ATIVA]

* **Origem:** ANS / Avisos de Beneficiário Identificado (ABI / FNS - Art. 32 da Lei 9.656/98).
* **Granularidade:** 1 registro por cobrança de ressarcimento (ABI / AIH).

| Coluna | Tipo SQL | Chave / Restrição | Descrição |
|---|---|---|---|
| `identificador_cobranca_abi` | `VARCHAR` | PK Única | Identificador do Aviso de Beneficiário Identificado. |
| `numero_aih` | `VARCHAR` | FK Única | Número da AIH que originou o atendimento hospitalar. |
| `codigo_estabelecimento_cnes` | `VARCHAR` | FK | Código CNES do hospital prestador. |
| `codigo_municipio_hospital` | `VARCHAR` | FK | Município de localização do hospital prestador. |
| `pseudonimo_paciente` | `VARCHAR` | FK | Pseudônimo criptográfico do paciente herdado deterministicamente do SIH. |
| `codigo_registro_ans` | `VARCHAR` | FK | Código de registro da operadora na ANS. |
| `razao_social_operadora` | `VARCHAR` | — | Razão social da operadora de saúde resolvida via Cadop. |
| `modalidade_operadora` | `VARCHAR` | — | Categoria regulatória da operadora. |
| `data_internacao` | `VARCHAR` | — | Data de admissão no leito hospitalar. |
| `data_alta` | `VARCHAR` | — | Data de alta hospitalar. |
| `codigo_cid10_principal` | `VARCHAR` | FK | Diagnóstico principal do atendimento. |
| `dias_permanencia_real` | `DOUBLE` | — | Total de dias de ocupação de leito. |
| `valor_notificado_brl` | `DOUBLE` | — | Valor total cobrado da operadora (R$). |
| `valor_recolhido_brl` | `DOUBLE` | — | Valor efetivamente recolhido ao FNS (R$ 0,00 para Impugnados e Recursos). |
| `situacao_cobranca` | `VARCHAR` | — | Situação processual da cobrança de ressarcimento. |
| `ano` | `VARCHAR` | Partição | Ano de competência do ressarcimento. |
| `mes` | `VARCHAR` | Partição | Mês de competência do ressarcimento. |
| `uf` | `VARCHAR` | Partição | Estado federativo do atendimento. |
| `id_execucao` | `VARCHAR` | — | Identificador de auditoria da execução do pipeline. |

---

## `dim_operadoras_saude` [STATUS: ENTREGUE / SILVER ATIVA]

* **Origem:** ANS / Cadop (Cadastro Nacional de Operadoras Ativas).
* **Granularidade:** 1 registro por operadora de plano de saúde ativa registrada na ANS.

| Coluna | Tipo SQL | Chave / Restrição | Descrição |
|---|---|---|---|
| `codigo_registro_ans` | `VARCHAR` | PK Única | Código oficial de registro da operadora na ANS. |
| `cnpj_operadora` | `VARCHAR` | — | CNPJ da operadora de saúde. |
| `razao_social` | `VARCHAR` | — | Razão Social da operadora registrada na ANS. |
| `nome_fantasia` | `VARCHAR` | Nullable | Nome comercial da operadora. |
| `modalidade_operadora` | `VARCHAR` | — | Modalidade/classificação regulatória da operadora. |
| `municipio_sede` | `VARCHAR` | — | Município da sede da operadora. |
| `uf_sede` | `VARCHAR` | — | UF da sede da operadora. |
| `cep` | `VARCHAR` | — | CEP do endereço da sede. |
| `status_operadora` | `VARCHAR` | — | Situação cadastral na ANS (`'ATIVA'`). |
| `data_registro_ans` | `VARCHAR` | — | Data de concessão do registro na ANS. |

---

## `dim_paciente` [STATUS: ENTREGUE / SILVER ATIVA]

* **Origem:** Master Patient Index (MPI em 4 Níveis) sobre bases DATASUS e ANS.
* **Granularidade:** 1 registro por paciente único identificado.

| Coluna | Tipo SQL | Chave / Restrição | Descrição |
|---|---|---|---|
| `pseudonimo_paciente` | `VARCHAR` | PK Única | Hash criptográfico irreversível do paciente (LGPD). |
| `identificador_paciente_candidato` | `VARCHAR` | — | Cluster probabilístico para cruzamento longitudinal. |
| `sexo_biologico` | `VARCHAR` | — | Sexo biológico harmonizado: `'M'`, `'F'` ou `'I'`. |
| `codigo_municipio_residencia` | `VARCHAR` | FK | Código IBGE do município de residência do paciente. |
| `uf_residencia` | `VARCHAR` | — | UF de residência do paciente. |

---

## `dim_tempo` [STATUS: ENTREGUE / SILVER ATIVA]

* **Origem:** Calendário contínuo de referência temporal gerado em memória.
* **Granularidade:** 1 registro por dia do calendário (período de 2025 a 2027).

| Coluna | Tipo SQL | Chave / Restrição | Descrição |
|---|---|---|---|
| `data` | `DATE` | PK Única | Data no formato ISO (AAAA-MM-DD). |
| `ano` | `INTEGER` | — | Ano numérico (ex: `2026`). |
| `mes` | `INTEGER` | — | Mês numérico (ex: `5`). |
| `dia` | `INTEGER` | — | Dia do mês (ex: `10`). |
| `nome_mes` | `VARCHAR` | — | Nome do mês por extenso. |
| `trimestre` | `VARCHAR` | — | Trimestre do ano (`'Q1'`, `'Q2'`, `'Q3'`, `'Q4'`). |
| `semestre` | `VARCHAR` | — | Semestre do ano (`'S1'`, `'S2'`). |
| `dia_semana` | `VARCHAR` | — | Nome do dia da semana. |
| `indicador_dia_util` | `BOOLEAN` | — | Booleano indicando dia útil (`TRUE`) ou fim de semana (`FALSE`). |

---

## `fct_glosas_hospitalares` [STATUS: EM IMPLEMENTAÇÃO / ROADMAP EXPANSÃO]

* **Origem:** DATASUS / SIHSUS (Subsistemas SIH-RJ e SIH-ER).
* **Granularidade:** 1 registro por AIH rejeitada / inconsistência de processamento com `CO_ERRO`.

| Coluna | Tipo SQL | Chave / Restrição | Descrição |
|---|---|---|---|
| `id_glosa_hospitalar` | `VARCHAR` | PK Única | Hash MD5 determinístico da glosa hospitalar. |
| `numero_aih` | `VARCHAR` | FK | Número da AIH rejeitada ou glosada. |
| `codigo_estabelecimento_cnes` | `VARCHAR` | FK | Código CNES do hospital prestador. |
| `codigo_municipio_hospital` | `VARCHAR` | FK | Município de localização do hospital. |
| `codigo_procedimento` | `VARCHAR` | FK | Procedimento SIGTAP apresentado na AIH recusada. |
| `valor_glosado_brl` | `DOUBLE` | — | Valor total da conta hospitalar recusada. |
| `codigo_motivo_glosa` | `VARCHAR` | — | Código numérico da crítica do DATASUS (`CO_ERRO`). |
| `descricao_motivo_glosa` | `VARCHAR` | — | Descrição textual da não-conformidade/glosa (`DS_ERRO`). |
| `tipo_origem_glosa` | `VARCHAR` | — | Identificador de origem (`'SIH_REJEICAO_SUS'`). |
| `ano` | `VARCHAR` | Partição | Ano de competência da rejeição. |
| `mes` | `VARCHAR` | Partição | Mês de competência da rejeição. |
| `uf` | `VARCHAR` | Partição | Estado federativo da rejeição. |
| `id_execucao` | `VARCHAR` | — | Identificador de auditoria da execução do pipeline. |

---

## `fct_glosas_tiss` [STATUS: EM IMPLEMENTAÇÃO / ROADMAP EXPANSÃO]

* **Origem:** ANS / Padrão TISS (Demonstrativos de Retorno e Análise de Contas Médicas Privadas).
* **Granularidade:** 1 registro por item de glosa privada da Tabela 38 (TUSS / ANS).

| Coluna | Tipo SQL | Chave / Restrição | Descrição |
|---|---|---|---|
| `registro_ans` | `VARCHAR` | FK | Código oficial de registro da operadora privada na ANS. |
| `numero_guia_prestador` | `VARCHAR` | — | Número unívoco da guia de faturamento do prestador. |
| `codigo_glosa_tuss` | `VARCHAR` | — | Código do motivo de glosa privada (Tabela 38 TUSS). |
| `descricao_glosa_tuss` | `VARCHAR` | — | Descrição oficial da não-conformidade TUSS. |
| `valor_apresentado_brl` | `DOUBLE` | — | Valor bruto cobrado pelo prestador privado. |
| `valor_glosado_brl` | `DOUBLE` | — | Valor recusado/glosado pela operadora. |
| `valor_liberado_brl` | `DOUBLE` | — | Valor líquido autorizado para pagamento. |
| `ano` | `VARCHAR` | Partição | Ano de competência da conta médica. |
| `mes` | `VARCHAR` | Partição | Mês de competência da conta médica. |

---

## `aud_alertas_anomalias` [STATUS: ENTREGUE / GOLD DW ATIVA]

* **Origem:** Motor analítico de auditoria clínica e financeira da Central de Anomalias.
* **Granularidade:** 1 registro por alerta candidato de anomalia identificado em internações hospitalares.

| Coluna | Tipo SQL | Chave / Restrição | Descrição |
|---|---|---|---|
| `id_alerta` | `VARCHAR` | PK Única | Hash MD5 determinístico do alerta de auditoria. |
| `numero_aih` | `VARCHAR` | FK | Número da AIH sob suspeita/auditoria. |
| `codigo_estabelecimento_cnes` | `VARCHAR` | FK | Código CNES do hospital prestador. |
| `uf` | `VARCHAR` | — | UF do atendimento. |
| `codigo_procedimento_realizado` | `VARCHAR` | FK | Código SIGTAP do procedimento faturado. |
| `tipo_anomalia` | `VARCHAR` | — | Tipo de regra violada (`'OUTLIER_CUSTO_P99'`, `'AIH_VALOR_ZERO'`, `'OBITO_PERMANENCIA_ZERO'`). |
| `severidade` | `VARCHAR` | — | Nível de criticidade do alerta (`'ALTA'`, `'MEDIA'`, `'CRITICA'`). |
| `valor_faturado_brl` | `DOUBLE` | — | Valor total da AIH faturado ao SUS. |
| `custo_esperado_brl` | `DOUBLE` | — | Custo de referência esperado (Percentil 90). |
| `excesso_custo_brl` | `DOUBLE` | — | Excesso faturado sobre o custo esperado. |
| `status_operacional` | `VARCHAR` | — | Status do workflow (`'NOVA'`, `'EM_ANALISE'`, `'RESOLVIDA'`, `'FALSO_POSITIVO'`). |
| `data_geracao` | `TIMESTAMP` | — | Data e hora de geração do alerta. |
| `versao_regra` | `VARCHAR` | — | Versão da regra analítica aplicada. |

---

## `agg_internacoes_uf` [STATUS: ENTREGUE / GOLD DW ATIVA]

* **Origem:** Data Mart analítico agregado da `fct_internacao`.
* **Granularidade:** 1 registro por UF e competência mensal.

| Coluna | Tipo SQL | Chave / Restrição | Descrição |
|---|---|---|---|
| `uf` | `VARCHAR` | PK Composta | Unidade da Federação. |
| `ano` | `VARCHAR` | PK Composta | Ano de competência. |
| `mes` | `VARCHAR` | PK Composta | Mês de competência. |
| `total_internacoes` | `BIGINT` | — | Total de internações hospitalares aprovadas. |
| `total_dias_internacao` | `BIGINT` | — | Somatório de dias de ocupação de leitos. |
| `media_dias_permanencia` | `DOUBLE` | — | Média de dias de permanência por internação. |
| `total_obitos` | `BIGINT` | — | Total de óbitos registrados no hospital. |
| `taxa_mortalidade_pct` | `DOUBLE` | — | Taxa bruta de mortalidade hospitalar (%). |
| `valor_total_brl` | `DOUBLE` | — | Valor total pago pelo SUS nas internações. |
| `valor_medio_internacao_brl` | `DOUBLE` | — | Custo médio faturado por AIH. |

---

## `agg_procedimentos_uf` [STATUS: ENTREGUE / GOLD DW ATIVA]

* **Origem:** Data Mart analítico agregado da `fct_atendimentos_ambulatoriais`.
* **Granularidade:** 1 registro por UF e competência mensal.

| Coluna | Tipo SQL | Chave / Restrição | Descrição |
|---|---|---|---|
| `uf` | `VARCHAR` | PK Composta | Unidade da Federação. |
| `ano` | `VARCHAR` | PK Composta | Ano de competência. |
| `mes` | `VARCHAR` | PK Composta | Mês de competência. |
| `total_atendimentos_ambulatoriais` | `BIGINT` | — | Total de linhas faturadas no SIA. |
| `total_procedimentos_produzidos` | `BIGINT` | — | Volume total de exames/consultas produzidos. |
| `total_procedimentos_aprovados` | `BIGINT` | — | Volume total de procedimentos homologados. |
| `valor_total_produzido_brl` | `DOUBLE` | — | Valor total cobrado na Tabela SUS. |
| `valor_total_aprovado_brl` | `DOUBLE` | — | Valor total aprovado e liquidado pelo SUS. |
| `valor_medio_procedimento_brl` | `DOUBLE` | — | Valor médio pago por procedimento aprovado. |

---

## `agg_perfil_epidemiologico` [STATUS: ENTREGUE / GOLD DW ATIVA]

* **Origem:** Data Mart analítico de prevalência clínica da `fct_internacao`.
* **Granularidade:** 1 registro por capítulo CID-10 e UF.

| Coluna | Tipo SQL | Chave / Restrição | Descrição |
|---|---|---|---|
| `capitulo_cid10` | `VARCHAR` | PK Composta | Diagnóstico principal ou agrupador de CID-10. |
| `uf` | `VARCHAR` | PK Composta | Unidade da Federação da internação. |
| `total_internacoes` | `BIGINT` | — | Total de pacientes internados sob o diagnóstico. |
| `total_obitos` | `BIGINT` | — | Total de desfechos fatais registrados. |
| `custo_total_brl` | `DOUBLE` | — | Gasto hospitalar acumulado no diagnóstico. |
