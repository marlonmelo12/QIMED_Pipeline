# Lista de Tarefas de Corre??o e Auditoria - QIMED Lakehouse

## Tarefa 1: Idempot?ncia e Deduplica??o da dim_tempo
- [x] **Passo 1:** Parametriza??o do Modo de Escrita em _persist_silver_table (adicionado par?metro mode: str = "append" com suporte a "overwrite" e schema_mode="overwrite").
- [x] **Passo 2:** Atualiza??o do M?todo gerar_dim_tempo para chamar explicitamente mode="overwrite".
- [x] **Passo 3:** Limpeza f?sica e regenera??o do Delta Lake Silver (lakehouse/silver/dim_tempo/) e sincroniza??o das tabelas no DuckDB (warehouse/qimed_silver_completa.duckdb e warehouse/qimed_dw.duckdb), garantindo exatamente 1.095 registros ?nicos.
- [x] **Passo 4:** Implementa??o e aprova??o com 100% de sucesso da suite de testes automatizados 	ests/test_dim_tempo.py (Unicidade, Granularidade e Idempot?ncia).

## Tarefas 2 e 3: Surrogate Key e Governan?a da Fato Ambulatorial SIA (ct_atendimentos_ambulatoriais)
- [x] **Passo 1:** Gera??o da Surrogate Key Determin?stica id_atendimento_ambulatorial como PK na query de extra??o Silver (src/processing/transformations.py).
- [x] **Passo 2:** Governan?a e blindagem terminol?gica no dicion?rio de dados (docs/dicionario_silver_duckdb.md), definindo granularidade de 1 linha por registro faturado e fixando os termos "Linhas Registradas" e "Quantidade Aprovada" para dashboards e APIs.
- [x] **Passo 3:** Preserva??o integral de procedimentos de valor R$ 0,00 da Aten??o B?sica (PAB) via COALESCE(TRY_CAST(PA_VALAPR AS DOUBLE), 0.0).

## Tarefa 4: Higieniza??o de Encoding ANS (Mojibake), Metadados UTF-8 e Modalidades Nulas
- [x] **Passo 1:** Decodifica??o resiliente no src/collectors/ans_collector.py via m?todo _decode_content_safe (UTF-8 com fallback e auto-corre??o de latin-1).
- [x] **Passo 2:** Sanitiza??o textual e tratamento de modalidades nulas (modalidade.fillna("N?O INFORMADA")) nos m?todos de parsing do AnsCollector.
- [x] **Passo 3:** Corre??o completa e reescrita do arquivo de metadados src/metadata/catalogo_dados.py para UTF-8 v?lido e leg?vel com acentua??o correta em todas as entidades.
- [x] **Passo 4:** Implementa??o e aprova??o com 100% de sucesso da suite de testes automatizados 	ests/test_ans_encoding.py (5/5 testes aprovados).

## Tarefa 5: Preserva??o de Continuidade da AIH e Desambigua??o Neonatal no MPI
- [x] **Passo 1:** Preserva??o dos campos de autoriza??o e continuidade hospitalar (
umero_documento_autorizacao, sequencial_aih5, motivo_saida, 
umero_aih_proxima, alor_servicos_hospitalares_brl, alor_servicos_profissionais_brl, codigo_procedimento_solicitado) em 	ransformar_sih_para_silver (src/processing/transformations.py).
- [x] **Passo 2:** Salvaguarda neonatal no Master Patient Index (src/mpi/patient_identity.py) incorporando o CNES e atendimento no hash para casos onde data_nascimento_paciente == data_internacao, prevenindo fus?o de prontu?rios de rec?m-nascidos distintos.
- [x] **Passo 3:** Implementa??o e aprova??o da suite de testes 	ests/test_sih_silver_pipeline.py (Preserva??o de AIH5 e Desambigua??o Neonatal).

## Tarefa 6: Harmoniza??o do Dom?nio de sexo_biologico e Resolu??o no MPI
- [x] **Passo 1:** Normaliza??o Can?nica na Extra??o do SIH (src/processing/transformations.py): mapeamento de '1'/'M' -> 'M', '2'/'3'/'F' -> 'F', outros/nulos -> 'I'.
- [x] **Passo 2:** Normaliza??o Can?nica na Extra??o do SIA (src/processing/transformations.py): mesma cl?usula padronizada.
- [x] **Passo 3:** Re-execu??o e persist?ncia f?sica das tabelas Silver particionadas (lakehouse/silver/fct_internacao, lakehouse/silver/fct_atendimentos_ambulatoriais, lakehouse/silver/dim_paciente) e sincroniza??o no DuckDB.
- [x] **Passo 4:** Implementa??o e aprova??o com 100% de sucesso da suite de testes automatizados 	ests/test_sexo_harmonizacao.py (Dom?nio Can?nico, Unifica??o Cross-System no MPI e N?o-Nulidade).

## Tarefa 7: Sanitiza??o de Diagn?stico Secund?rio Sentinela e Governan?a Cl?nica
- [x] **Passo 1:** Sanitiza??o de DIAG_SECUN em ('0000', '0', '', 'NONE', 'NULL', 'nan') para NULL no SIH (src/processing/transformations.py).
- [x] **Passo 2:** Documenta??o no dicion?rio de dados (docs/dicionario_silver_duckdb.md) formalizando a governan?a cl?nica que veda o uso de codigo_cid10_secundario como feature preditora devido ao sub-registro na base reduzida.
- [x] **Passo 3:** Valida??o com 100% de sucesso nos testes automatizados 	ests/test_sih_silver_pipeline.py.

## Tarefa 8: Sem?ntica de Ressarcimento ao SUS e Isolamento de Glosas TISS
- [x] **Passo 1:** Corre??o do fallback de status_cobranca no AnsCollector (src/collectors/ans_collector.py) de "RECOLHIDO" para "EM_ANALISE".
- [x] **Passo 2:** Padroniza??o da taxonomia de status e garantia de coer?ncia financeira (l_recolhido_brl = 0.0 para cobran?as com status IMPUGNADO).
- [x] **Passo 3:** Documenta??o no dicion?rio de dados (docs/dicionario_silver_duckdb.md) formalizando a separa??o conceitual estrita entre Ressarcimento ao SUS (Art. 32 Lei 9.656/98 / FNS) e Glosas Privadas da Sa?de Suplementar (Padr?o TISS).
- [x] **Passo 4:** Implementa??o e aprova??o com 100% de sucesso da suite de testes automatizados 	ests/test_ans_ressarcimento.py (16/16 testes aprovados).

## Tarefa 9: Conectores SIH-RJ, SIH-ER e Fato Glosas Hospitalares (ct_glosas_hospitalares)
- [x] **Passo 1:** Expans?o do DatasusCollector (src/collectors/datasus_collector.py) para suportar os subsistemas SIH-RJ (prefixo RJ) e SIH-ER (prefixo ER).
- [x] **Passo 2:** Cria??o do m?todo 	ransformar_glosas_hospitalares_para_silver em src/processing/transformations.py para correlacionar AIHs rejeitadas aos motivos de glosa do DATASUS.
- [x] **Passo 3:** Extens?o do Data Mart de glosas (src/gold/models/kpi_glosas_auditoria.py) com a fun??o uild_dm_motivos_glosas_hospitalares.
- [x] **Passo 4:** Documenta??o completa da tabela ct_glosas_hospitalares no dicion?rio de dados (docs/dicionario_silver_duckdb.md).
- [x] **Passo 5:** Implementa??o e aprova??o com 100% de sucesso da suite de testes automatizados 	ests/test_sih_rejeicoes.py (3/3 testes aprovados).

## Tarefa 10: Agrupamento Can?nico dos 22 Cap?tulos da CID-10
- [x] **Passo 1:** Adi??o do Cap?tulo XXII ("XXII": ("U00", "U99", "C?digos para situa??es especiais (COVID-19)")) no dicion?rio CID10_CHAPTERS em src/silver/terminology.py.
- [x] **Passo 2:** Refatora??o de 
esolver_cid10_nacional em src/silver/cid10_nacional.py substituindo checagem por 1? letra por avalia??o estrita de faixas de 3 caracteres (cod_clean[:3]).
- [x] **Passo 3:** Desambigua??o das faixas compartilhadas em letras cr?ticas: D00-D48 (Neoplasias) vs D50-D89 (Sangue); H00-H59 (Olhos) vs H60-H95 (Ouvidos).
- [x] **Passo 4:** Implementa??o e aprova??o com 100% de sucesso da suite de testes automatizados 	ests/test_cid10_capitulos.py (Desambigua??o D/H, Cobertura Integral dos 22 Cap?tulos, Normaliza??o com/sem ponto e Valida??o do Cap?tulo XXII no TerminologyService).

## Tarefa 11: Diverg?ncias Territoriais (IBGE vs UF) e Mobilidade Assistencial
- [x] **Passo 1:** Deriva??o determin?stica de uf_residencia_paciente na ct_internacao e de uf_residencia na dim_paciente no SIH (src/processing/transformations.py) via prefixo de 2 d?gitos do c?digo IBGE.
- [x] **Passo 2:** Deriva??o determin?stica de uf_residencia_paciente na ct_atendimentos_ambulatoriais no SIA (src/processing/transformations.py) via prefixo de 2 d?gitos do c?digo IBGE.
- [x] **Passo 3:** Preserva??o integral dos fluxos de mobilidade assistencial interestadual (PPI - Programa??o Pactuada e Integrada) onde uf_residencia_paciente != uf.
- [x] **Passo 4:** Implementa??o e aprova??o com 100% de sucesso da suite de testes automatizados 	ests/test_territorial_ibge.py (Cobertura 27 UFs, Corre??o de Inconsist?ncias e Preserva??o de Mobilidade Interestadual).

## Tarefa 12: Sanitiza??o de Sentinelas do DATASUS SIA (Idade 999 e CID 0000)
- [x] **Passo 1:** Sanitiza??o de idade sentinela PA_IDADE = 999 e valores negativos para NULL no SIA (src/processing/transformations.py).
- [x] **Passo 2:** Sanitiza??o de CIDs sentinelas PA_CIDPRI e PA_CIDSEC em ('0000', '0', '', 'NONE', 'NULL', 'nan') para NULL.
- [x] **Passo 3:** Implementa??o e aprova??o com 100% de sucesso da suite de testes automatizados 	ests/test_sia_silver_pipeline.py (Unicidade de PK, Sanitiza??o de Idade Sentinela, Sanitiza??o de CIDs Sentinela e Preserva??o de Procedimentos R$ 0,00).

## Tarefa 13: Central de Anomalias e Auditoria Hospitalar (ud_alertas_anomalias)
- [x] **Passo 1:** Cria??o do motor anal?tico vetorizado src/gold/models/kpi_central_anomalias.py implementando as 3 regras can?nicas (Outliers $> P_{99}$, AIHs Iniciais com Valor Zero e ?bitos Imediatos / Perman?ncia Zero).
- [x] **Passo 2:** C?lculo da m?trica oficial "Excesso em Rela??o ao Custo Esperado" ($\max(0.0, 	ext{valor\_faturado\_brl} - P_{90})$) e gest?o desacoplada do ciclo de vida operacional (NOVA, EM_ANALISE, RESOLVIDA, FALSO_POSITIVO).
- [x] **Passo 3:** Integra??o da materializa??o de ud_alertas_anomalias no pipeline Gold (src/gold/pipeline_nacional.py).
- [x] **Passo 4:** Documenta??o completa da tabela de auditoria no dicion?rio de dados (docs/dicionario_silver_duckdb.md).
- [x] **Passo 5:** Implementa??o e aprova??o com 100% de sucesso da suite de testes automatizados 	ests/test_central_anomalias.py (4/4 testes aprovados).
