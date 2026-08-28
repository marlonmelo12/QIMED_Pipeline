"""
Transformacoes Silver - Star Schema 100% em Portugues - QIMED Lakehouse V3.
Executa transformacoes out-of-core via DuckDB Engine com particionamento inteligente por UF,
sem converter para Pandas (Pure PyArrow Zero-Copy).
"""
import os
import time
from typing import Any, Dict, List, Optional
import pyarrow as pa
from deltalake.writer import write_deltalake

from src.processing.duckdb_engine import DuckDBEngine
from src.processing.lineage_tracker import DataLineageTracker
from src.mpi.patient_identity import PatientIdentityResolver
from src.quality.schema_validator import SchemaValidator
from src.schemas.sih.silver_schema import MAPEAMENTO_SIH_SILVER
from src.schemas.sia.silver_schema import MAPEAMENTO_SIA_SILVER
from src.utils.logging_config import setup_logger
from src.utils.config_loader import load_pipeline_config

logger = setup_logger(__name__)

UF_LIST = [
    "RO", "AC", "AM", "RR", "PA", "AP", "TO",
    "MA", "PI", "CE", "RN", "PB", "PE", "AL", "SE", "BA",
    "MG", "ES", "RJ", "SP", "PR", "SC", "RS", "MS", "MT", "GO", "DF"
]


class CanonicalTransformations:
    """
    Aplica as regras clinicas e estruturais do Star Schema de Saude em Portugues com PyArrow puro.
    """

    def __init__(self, duck_engine: Optional[DuckDBEngine] = None, config: Optional[Dict[str, Any]] = None):
        self.cfg = config or load_pipeline_config()
        self.engine = duck_engine or DuckDBEngine(config=self.cfg)
        self.silver_path = self.cfg.get("paths", {}).get("silver_dir", "lakehouse/silver")
        self.bronze_path = self.cfg.get("paths", {}).get("bronze_dir", "lakehouse/bronze")
        self.lineage_tracker = DataLineageTracker()
        self.mpi_resolver = PatientIdentityResolver(duck_engine=self.engine, config=self.cfg)
        os.makedirs(self.silver_path, exist_ok=True)

    def _persist_silver_table(
        self,
        table_name: str,
        arrow_table: pa.Table,
        execution_id: str,
        source_entity: str,
        partition_by: Optional[List[str]] = None,
        mode: str = "append",
        predicate: Optional[str] = None,
    ):
        """
        Persiste uma Arrow Table na camada Silver (Delta Lake).

        Quando `predicate` e `mode="overwrite"` sao fornecidos juntos, o Delta Lake executa
        um Dynamic Partition Overwrite: apenas as linhas que satisfazem o predicado sao
        substituidas, preservando todas as outras particoes intactas.
        Isso garante idempotencia estrita por particao, independente da ordem de chamada.
        """
        t_path = os.path.join(self.silver_path, table_name)

        # [CORRECAO-17] Classificacao de excecoes:
        # - OSError/IOError: fatais (disco, permissao) -> re-raise imediato.
        # - Demais: logar com stack trace completo e re-raise para o orquestrador decidir.
        try:
            write_kwargs = dict(
                mode=mode,
                partition_by=partition_by,
                schema_mode="overwrite" if mode == "overwrite" else "merge",
            )
            if predicate is not None:
                write_kwargs["predicate"] = predicate

            write_deltalake(t_path, arrow_table, **write_kwargs)

            logger.info(
                f"[SILVER PERSISTED] {table_name}: {len(arrow_table):,} linhas "
                f"gravadas em Delta Silver (PyArrow Puro)."
                + (f" Predicado: [{predicate}]" if predicate else "")
            )
            self.lineage_tracker.record_lineage(
                execution_id=execution_id,
                source_layer="Bronze/Staging",
                source_entity=source_entity,
                target_layer="Silver",
                target_entity=table_name,
                rows_transformed=len(arrow_table),
            )
        except (OSError, IOError) as e:
            logger.error(
                f"[FATAL I/O] Falha de disco/permissao ao gravar Silver '{table_name}': {e}",
                exc_info=True,
            )
            raise
        except Exception as e:
            logger.error(
                f"[ERRO] Falha ao gravar Silver '{table_name}': {e}",
                exc_info=True,
            )
            raise

    def gerar_dim_tempo(self, start_year: int = 2025, end_year: int = 2027, execution_id: str = "exec_init"):
        sql = f"""
        SELECT
            d::DATE AS data,
            EXTRACT(YEAR FROM d)::INTEGER AS ano,
            EXTRACT(MONTH FROM d)::INTEGER AS mes,
            EXTRACT(DAY FROM d)::INTEGER AS dia,
            STRFTIME(d, '%B') AS nome_mes,
            'Q' || EXTRACT(QUARTER FROM d) AS trimestre,
            CASE WHEN EXTRACT(MONTH FROM d) <= 6 THEN 'S1' ELSE 'S2' END AS semestre,
            STRFTIME(d, '%A') AS dia_semana,
            CASE WHEN EXTRACT(DOW FROM d) IN (0, 6) THEN FALSE ELSE TRUE END AS indicador_dia_util
        FROM generate_series(DATE '{start_year}-01-01', DATE '{end_year}-12-31', INTERVAL '1 DAY') AS t(d)
        """
        arr = self.engine.fetch_arrow(sql)
        self._persist_silver_table("dim_tempo", arr, execution_id, "generate_series", mode="overwrite")
        del arr

    def transformar_ans_para_silver(self, execution_id: str = "exec_ans"):
        """
        Transforma e persiste dim_operadoras_saude e fct_ressarcimento_sus na camada Silver (Delta Lake),
        garantindo sanitização de mojibake, preenchimento de modalidade com 'NÃO INFORMADA',
        resolução relacional de razão social via dim_operadoras_saude limpa, trava de impugnações e persistência com mode='overwrite'.
        """
        from src.collectors.ans_collector import AnsCollector
        collector = AnsCollector(modalidade="operadoras")

        # 1. dim_operadoras_saude
        ans_operadoras_bronze = os.path.join(self.bronze_path, "ans", "operadoras").replace(chr(92), "/")
        if os.path.exists(ans_operadoras_bronze):
            df_op = self.engine.query(f"SELECT * FROM delta_scan('{ans_operadoras_bronze}')").df()
            mapa_op = {
                "cd_operadora": "codigo_registro_ans",
                "Registro_ANS": "codigo_registro_ans",
                "REGISTRO_OPERADORA": "codigo_registro_ans",
                "cnpj": "cnpj_operadora",
                "CNPJ": "cnpj_operadora",
                "modalidade": "modalidade_operadora",
                "MODALIDADE": "modalidade_operadora",
                "municipio": "municipio_sede",
                "CIDADE": "municipio_sede",
                "uf": "uf_sede",
                "UF": "uf_sede",
                "situacao": "status_operadora",
                "SITUACAO": "status_operadora",
                "dt_registro_ans": "data_registro_ans",
                "DATA_REGISTRO_ANS": "data_registro_ans",
            }
            df_op = df_op.rename(columns={k: v for k, v in mapa_op.items() if k in df_op.columns})

            for col in ["razao_social", "nome_fantasia", "modalidade_operadora", "municipio_sede"]:
                if col in df_op.columns:
                    df_op[col] = df_op[col].apply(collector._fix_text_mojibake)
                    df_op[col] = df_op[col].astype(str).str.strip().str.replace(r"\s+", " ", regex=True)
                    df_op.loc[df_op[col].isin(["", "None", "nan", "NULL", "none", "<NA>"]), col] = None

            if "modalidade_operadora" in df_op.columns:
                df_op["modalidade_operadora"] = df_op["modalidade_operadora"].fillna("NÃO INFORMADA")

            if "codigo_registro_ans" in df_op.columns:
                df_op["codigo_registro_ans"] = df_op["codigo_registro_ans"].astype(str).str.zfill(6)

            col_order = ["codigo_registro_ans", "cnpj_operadora", "razao_social", "nome_fantasia", "modalidade_operadora", "municipio_sede", "uf_sede", "cep", "status_operadora", "data_registro_ans"]
            cols_to_keep = [c for c in col_order if c in df_op.columns]
            df_op = df_op[cols_to_keep]

            arr_op = pa.Table.from_pandas(df_op, preserve_index=False)
            self._persist_silver_table("dim_operadoras_saude", arr_op, execution_id, "bronze_ans_operadoras", mode="overwrite")
            del arr_op

        # 2. fct_ressarcimento_sus com Resolução Relacional e Bridge MPI com SIH
        dim_op_silver = os.path.join(self.silver_path, "dim_operadoras_saude").replace(chr(92), "/")
        sih_silver_path = os.path.join(self.silver_path, "fct_internacao").replace(chr(92), "/")
        raw_rss_path = os.path.join(self.silver_path, "fct_ressarcimento_sus").replace(chr(92), "/")
        if not os.path.exists(raw_rss_path):
            raw_rss_path = os.path.join(self.bronze_path, "ans", "ressarcimento").replace(chr(92), "/")

        if os.path.exists(raw_rss_path) and os.path.exists(dim_op_silver):
            if os.path.exists(sih_silver_path):
                sih_cte = f"""
                WITH aih_paciente_map AS (
                    -- Mapeamento canônico 1:1 por AIH para herança do MPI
                    SELECT 
                        numero_aih,
                        ANY_VALUE(pseudonimo_paciente) AS pseudonimo_paciente_sih,
                        ANY_VALUE(codigo_estabelecimento_cnes) AS cnes_sih,
                        ANY_VALUE(codigo_municipio_hospital) AS munic_hosp_sih
                    FROM delta_scan('{sih_silver_path}')
                    WHERE numero_aih IS NOT NULL
                    GROUP BY numero_aih
                )
                """
                sih_join = "LEFT JOIN aih_paciente_map m ON TRIM(CAST(f.numero_aih AS VARCHAR)) = TRIM(CAST(m.numero_aih AS VARCHAR))"
                pseudo_expr = "COALESCE(m.pseudonimo_paciente_sih, f.pseudonimo_paciente) AS pseudonimo_paciente"
                cnes_expr = "COALESCE(f.codigo_estabelecimento_cnes, m.cnes_sih, '0000000') AS codigo_estabelecimento_cnes"
                munic_expr = "COALESCE(f.codigo_municipio_hospital, m.munic_hosp_sih, '000000') AS codigo_municipio_hospital"
            else:
                sih_cte = ""
                sih_join = ""
                pseudo_expr = "f.pseudonimo_paciente"
                cnes_expr = "f.codigo_estabelecimento_cnes"
                munic_expr = "f.codigo_municipio_hospital"

            sql_rss = f"""
            {sih_cte}
            SELECT 
                f.identificador_cobranca_abi,
                f.numero_aih,
                {cnes_expr},
                {munic_expr},
                {pseudo_expr},
                f.codigo_registro_ans,
                COALESCE(o.razao_social, f.razao_social_operadora) AS razao_social_operadora,
                COALESCE(o.modalidade_operadora, f.modalidade_operadora) AS modalidade_operadora,
                f.data_internacao,
                f.data_alta,
                f.codigo_cid10_principal,
                TRY_CAST(f.dias_permanencia_real AS DOUBLE) AS dias_permanencia_real,
                TRY_CAST(f.valor_notificado_brl AS DOUBLE) AS valor_notificado_brl,
                -- 2. [TASK 2.2] Ajuste Contábil: Zerar valor recolhido para IMPUGNADO e EM RECURSO
                CASE 
                    WHEN UPPER(TRIM(CAST(f.situacao_cobranca AS VARCHAR))) LIKE '%IMPUGNAD%' THEN 0.0 
                    WHEN UPPER(TRIM(CAST(f.situacao_cobranca AS VARCHAR))) LIKE '%RECURSO%' THEN 0.0
                    ELSE COALESCE(TRY_CAST(f.valor_recolhido_brl AS DOUBLE), 0.0)
                END AS valor_recolhido_brl,
                CASE
                    WHEN f.situacao_cobranca IS NULL OR TRIM(CAST(f.situacao_cobranca AS VARCHAR)) IN ('', 'None', 'nan', 'NULL', 'NONE', 'NAN') THEN 'EM_ANALISE'
                    ELSE TRIM(CAST(f.situacao_cobranca AS VARCHAR))
                END AS situacao_cobranca,

                -- [CORRECAO-11] Flag de Integridade Contabil ANS
                -- Detecta cobranças declaradas como PAGO mas com valor_recolhido = 0.
                -- Indica inconsistencia entre situacao_cobranca e realidade financeira.
                -- Usado em kpi_central_anomalias e auditoria ANS.
                CASE
                    WHEN UPPER(TRIM(CAST(f.situacao_cobranca AS VARCHAR))) LIKE '%PAGO%'
                     AND COALESCE(TRY_CAST(f.valor_recolhido_brl AS DOUBLE), 0.0) = 0.0
                    THEN TRUE
                    ELSE FALSE
                END AS flag_anomalia_contabil_ans,

                f.uf, f.ano, f.mes,
                '{execution_id}' AS id_execucao
            FROM delta_scan('{raw_rss_path}') f
            {sih_join}
            LEFT JOIN delta_scan('{dim_op_silver}') o ON TRIM(CAST(f.codigo_registro_ans AS VARCHAR)) = TRIM(CAST(o.codigo_registro_ans AS VARCHAR))
            """
            try:
                arr_rss = self.engine.fetch_arrow(sql_rss)
                self._persist_silver_table("fct_ressarcimento_sus", arr_rss, execution_id, "ans_ressarcimento", partition_by=["ano", "mes", "uf"], mode="overwrite")
                del arr_rss
            except Exception as e:
                # [CORRECAO-17] Falha no ressarcimento ANS e critica — re-raise.
                logger.error(
                    f"[ERRO] Falha na transformacao de fct_ressarcimento_sus: {e}",
                    exc_info=True,
                )
                raise

    def transformar_sih_para_silver(self, execution_id: str = "exec_sih"):
        sih_bronze_delta = os.path.join(self.bronze_path, "datasus", "sih").replace(chr(92), "/")
        if not os.path.exists(sih_bronze_delta):
            logger.warning("Tabela Bronze SIH nao encontrada para transformacao Silver.")
            return

        cols_df = self.engine.query(f"DESCRIBE SELECT * FROM delta_scan('{sih_bronze_delta}')").df()
        cols_available = set(cols_df["column_name"].str.upper().tolist())

        doc_expr = (
            "TRY_CAST(NUM_DOC AS VARCHAR)" if "NUM_DOC" in cols_available
            else ("TRY_CAST(N_DOC AS VARCHAR)" if "N_DOC" in cols_available
            else ("TRY_CAST(NUM_PROC AS VARCHAR)" if "NUM_PROC" in cols_available
            else "NULL"))
        )
        seq_aih5_expr = "TRY_CAST(SEQ_AIH5 AS INTEGER)" if "SEQ_AIH5" in cols_available else "NULL"
        motsaid_expr = (
            "TRY_CAST(MOTSAID AS VARCHAR)" if "MOTSAID" in cols_available
            else ("TRY_CAST(COBRANCA AS VARCHAR)" if "COBRANCA" in cols_available
            else "NULL")
        )
        aihprox_expr = "TRY_CAST(AIH_PROX AS VARCHAR)" if "AIH_PROX" in cols_available else "CAST(NULL AS VARCHAR)"
        val_sh_expr = "TRY_CAST(VAL_SH AS DOUBLE)" if "VAL_SH" in cols_available else "0.0"
        val_sp_expr = "TRY_CAST(VAL_SP AS DOUBLE)" if "VAL_SP" in cols_available else "0.0"
        proc_solic_expr = "COALESCE(TRY_CAST(PROC_SOLIC AS VARCHAR), PROC_REA)" if "PROC_SOLIC" in cols_available else "PROC_REA"

        raw_sql = f"""
        SELECT
            *,
            N_AIH AS numero_aih,
            {doc_expr} AS numero_documento_autorizacao,
            CNES AS codigo_estabelecimento_cnes,
            DT_INTER AS data_internacao,
            NASC AS data_nascimento_paciente,
            CASE 
                WHEN UPPER(TRIM(CAST(SEXO AS VARCHAR))) IN ('1', 'M', 'MASC', 'MASCULINO') THEN 'M'
                WHEN UPPER(TRIM(CAST(SEXO AS VARCHAR))) IN ('2', '3', 'F', 'FEM', 'FEMININO') THEN 'F'
                ELSE 'I'
            END AS sexo_biologico,
            MUNIC_RES AS codigo_municipio_residencia_paciente,
            {seq_aih5_expr} AS seq_aih5_val,
            {motsaid_expr} AS motsaid_val,
            {aihprox_expr} AS aihprox_val,
            {val_sh_expr} AS val_sh_val,
            {val_sp_expr} AS val_sp_val,
            {proc_solic_expr} AS proc_solic_val
        FROM delta_scan('{sih_bronze_delta}')
        """
        resolved_sql = self.mpi_resolver.resolve_identities_sql(raw_sql)

        final_sql = f"""
        SELECT
            -- 1. [CORRECAO-06] Chave Primaria Tecnica Deterministica (Surrogate Key)
            -- ANTES: md5(..., ROW_NUMBER() OVER ()) -- nao deterministico entre execucoes.
            -- DEPOIS: sha256 sobre campos naturais de negocio que identificam unicamente
            --         cada faturamento de AIH dentro de uma competencia (ano/mes/uf).
            sha256(concat_ws('-',
                COALESCE(N_AIH,    ''),
                COALESCE(IDENT,    ''),
                COALESCE(CNES,     ''),
                COALESCE(PROC_REA, ''),
                COALESCE(DT_SAIDA, ''),
                COALESCE(ano,      ''),
                COALESCE(mes,      ''),
                COALESCE(uf,       '')
            )) AS id_internacao_hospitalar,

            -- 2. [TASK 1.3] Chave de Episódio Clínico Contínuo (Vincula AIH 1 inicial e AIHs 5 subsequentes)
            md5(concat_ws('-', 
                COALESCE(f.pseudonimo_paciente, ''), 
                COALESCE(CNES, ''), 
                COALESCE(DT_INTER, '')
            )) AS id_episodio_internacao,

            N_AIH AS numero_aih,
            f.numero_documento_autorizacao,
            f.seq_aih5_val AS sequencial_aih5,
            IDENT AS tipo_identificacao_aih,
            f.motsaid_val AS motivo_saida,
            
            -- 3. [TASK 1.2] Tipagem Correta como VARCHAR (Preserva 13 dígitos e zeros à esquerda)
            CAST(f.aihprox_val AS VARCHAR) AS numero_aih_proxima,
            
            CNES AS codigo_estabelecimento_cnes,
            MUNIC_RES AS codigo_municipio_residencia_paciente,
            
            -- Derivação Territorial IBGE da UF de Residência
            CASE SUBSTRING(TRIM(CAST(MUNIC_RES AS VARCHAR)), 1, 2)
                WHEN '11' THEN 'RO' WHEN '12' THEN 'AC' WHEN '13' THEN 'AM' WHEN '14' THEN 'RR' WHEN '15' THEN 'PA' WHEN '16' THEN 'AP' WHEN '17' THEN 'TO'
                WHEN '21' THEN 'MA' WHEN '22' THEN 'PI' WHEN '23' THEN 'CE' WHEN '24' THEN 'RN' WHEN '25' THEN 'PB' WHEN '26' THEN 'PE' WHEN '27' THEN 'AL' WHEN '28' THEN 'SE' WHEN '29' THEN 'BA'
                WHEN '31' THEN 'MG' WHEN '32' THEN 'ES' WHEN '33' THEN 'RJ' WHEN '35' THEN 'SP'
                WHEN '41' THEN 'PR' WHEN '42' THEN 'SC' WHEN '43' THEN 'RS'
                WHEN '50' THEN 'MS' WHEN '51' THEN 'MT' WHEN '52' THEN 'GO' WHEN '53' THEN 'DF'
                ELSE uf
            END AS uf_residencia_paciente,
            
            MUNIC_MOV AS codigo_municipio_hospital,
            NASC AS data_nascimento_paciente,
            f.sexo_biologico,
            
            DT_INTER AS data_internacao,
            DT_SAIDA AS data_alta,
            
            DIAG_PRINC AS codigo_cid10_principal,
            
            -- Sanitização do CID Secundário
            -- [CORRECAO-10] Sentinelas expandidas: inclui variantes de NA geradas por
            -- pandas/pyarrow (NAN, <NA>, none, N/A) e padrão de zeros arbitrários (^0+$).
            CASE
                WHEN TRIM(CAST(DIAG_SECUN AS VARCHAR)) IN (
                    '0000', '0', '', 'NONE', 'NULL', 'nan', 'NAN', '<NA>', 'none', 'N/A', 'na'
                )
                 OR regexp_matches(TRIM(CAST(DIAG_SECUN AS VARCHAR)), '^0+$')
                THEN NULL
                ELSE TRIM(CAST(DIAG_SECUN AS VARCHAR))
            END AS codigo_cid10_secundario,
            
            -- 4. [TASK 1.3] Decomposição Semântica de Permanência
            TRY_CAST(DIAS_PERM AS INTEGER) AS dias_permanencia_real,
            TRY_CAST(DIAS_PERM AS INTEGER) AS dias_permanencia_faturados_mes,
            CASE 
                WHEN LENGTH(TRIM(CAST(DT_INTER AS VARCHAR))) = 8 AND LENGTH(TRIM(CAST(DT_SAIDA AS VARCHAR))) = 8 
                THEN DATEDIFF('day', STRPTIME(CAST(DT_INTER AS VARCHAR), '%Y%m%d'), STRPTIME(CAST(DT_SAIDA AS VARCHAR), '%Y%m%d'))
                ELSE TRY_CAST(DIAS_PERM AS INTEGER)
            END AS dias_duracao_acumulada_episodio,
            
            CASE WHEN MORTE = '1' THEN TRUE ELSE FALSE END AS indicador_obito,
            
            TRY_CAST(VAL_TOT AS DOUBLE) AS valor_total_brl,
            TRY_CAST(VAL_UTI AS DOUBLE) AS valor_uti_brl,
            f.val_sh_val AS valor_servicos_hospitalares_brl,
            f.val_sp_val AS valor_servicos_profissionais_brl,
            
            PROC_REA AS codigo_procedimento_realizado,
            f.proc_solic_val AS codigo_procedimento_solicitado,
            
            ano, mes, uf,
            '{execution_id}' AS id_execucao,
            f.identificador_atendimento,
            f.identificador_registro,
            f.pseudonimo_paciente,
            f.identificador_paciente_candidato
        FROM ({resolved_sql}) f
        """
        try:
            arr_internacao = self.engine.fetch_arrow(final_sql)
            self._persist_silver_table("fct_internacao", arr_internacao, execution_id, "bronze_sih", partition_by=["ano", "mes", "uf"], mode="overwrite")
            del arr_internacao
        except Exception as e:
            # [CORRECAO-17] fct_internacao e a tabela principal do SIH — falha e fatal.
            logger.error(
                f"[ERRO] Falha na transformacao de fct_internacao: {e}",
                exc_info=True,
            )
            raise

        sql_dim_paciente = f"""
        SELECT DISTINCT
            pseudonimo_paciente,
            identificador_paciente_candidato,
            sexo_biologico,
            codigo_municipio_residencia_paciente AS codigo_municipio_residencia,
            uf_residencia_paciente AS uf_residencia
        FROM ({final_sql})
        WHERE data_nascimento_paciente IS NOT NULL OR codigo_municipio_residencia_paciente IS NOT NULL
        """
        try:
            arr_paciente = self.engine.fetch_arrow(sql_dim_paciente)
            self._persist_silver_table("dim_paciente", arr_paciente, execution_id, "bronze_sih", mode="overwrite")
            del arr_paciente
        except Exception as e:
            # [CORRECAO-17] Falha em dim_paciente deixa a dimensao desatualizada — re-raise.
            logger.error(
                f"[ERRO] Falha na transformacao de dim_paciente: {e}",
                exc_info=True,
            )
            raise

    def transformar_sia_para_silver(self, execution_id: str = "exec_sia"):
        sia_bronze_delta = os.path.join(self.bronze_path, "datasus", "sia").replace(chr(92), "/")
        if not os.path.exists(sia_bronze_delta):
            logger.warning("Tabela Bronze SIA nao encontrada para transformacao Silver.")
            return

        cols_df = self.engine.query(f"DESCRIBE SELECT * FROM delta_scan('{sia_bronze_delta}')").df()
        cols_available = set(cols_df["column_name"].str.upper().tolist())
        dtocor_expr = "COALESCE(TRY_CAST(PA_DTOCOR AS VARCHAR), '')" if "PA_DTOCOR" in cols_available else ("COALESCE(TRY_CAST(PA_MVM AS VARCHAR), TRY_CAST(PA_CMP AS VARCHAR), '')" if "PA_MVM" in cols_available else "''")

        total_sia_transformed = 0
        for idx, uf in enumerate(UF_LIST):
            sql_uf = f"""
            SELECT
                -- [CORRECAO-07] Surrogate Key Deterministica (PK do SIA)
                -- ANTES: md5(..., ROW_NUMBER() OVER ()) -- nao deterministico entre execucoes.
                -- DEPOIS: sha256 sobre campos que identificam unicamente o atendimento:
                --   PA_CODUNI  = CNES do estabelecimento
                --   PA_PROC_ID = procedimento SIGTAP
                --   PA_CMP     = competencia (AAAAMM) do atendimento
                --   PA_MUNPCN  = municipio de residencia do paciente
                --   PA_CNS_PAC = Cartao Nacional de Saude (anonimizado na fonte)
                --   PA_SEXO    = sexo biologico
                --   PA_IDADE   = idade (discrimina atendimentos identicos no mesmo dia)
                sha256(concat_ws('-',
                    COALESCE(TRY_CAST(PA_CODUNI  AS VARCHAR), ''),
                    COALESCE(TRY_CAST(PA_PROC_ID AS VARCHAR), ''),
                    COALESCE(TRY_CAST(PA_CMP     AS VARCHAR), {dtocor_expr}),
                    COALESCE(TRY_CAST(PA_MUNPCN  AS VARCHAR), ''),
                    COALESCE(TRY_CAST(PA_CNS_PAC AS VARCHAR), ''),
                    COALESCE(TRY_CAST(PA_SEXO    AS VARCHAR), ''),
                    COALESCE(TRY_CAST(PA_IDADE   AS VARCHAR), '')
                )) AS id_atendimento_ambulatorial,
                
                PA_CODUNI AS codigo_estabelecimento_cnes,
                PA_GESTAO AS codigo_gestor,
                PA_UFMUN AS codigo_municipio_estabelecimento,
                PA_PROC_ID AS codigo_procedimento_sigtap,
                
                -- [CORRECAO-10] Sanitização de CIDs Sentinela (SIA)
                -- Sentinelas expandidas: NAN, <NA>, none, N/A e regex '^0+$'.
                CASE
                    WHEN TRIM(CAST(PA_CIDPRI AS VARCHAR)) IN (
                        '0000', '0', '', 'NONE', 'NULL', 'nan', 'NAN', '<NA>', 'none', 'N/A', 'na'
                    )
                     OR regexp_matches(TRIM(CAST(PA_CIDPRI AS VARCHAR)), '^0+$')
                    THEN NULL
                    ELSE TRIM(CAST(PA_CIDPRI AS VARCHAR))
                END AS codigo_cid10_principal,

                CASE
                    WHEN TRIM(CAST(PA_CIDSEC AS VARCHAR)) IN (
                        '0000', '0', '', 'NONE', 'NULL', 'nan', 'NAN', '<NA>', 'none', 'N/A', 'na'
                    )
                     OR regexp_matches(TRIM(CAST(PA_CIDSEC AS VARCHAR)), '^0+$')
                    THEN NULL
                    ELSE TRIM(CAST(PA_CIDSEC AS VARCHAR))
                END AS codigo_cid10_secundario,
                
                -- [TASK 6] Harmonização Canônica de Sexo
                CASE 
                    WHEN UPPER(TRIM(CAST(PA_SEXO AS VARCHAR))) IN ('1', 'M', 'MASC', 'MASCULINO') THEN 'M'
                    WHEN UPPER(TRIM(CAST(PA_SEXO AS VARCHAR))) IN ('2', '3', 'F', 'FEM', 'FEMININO') THEN 'F'
                    ELSE 'I'
                END AS sexo_biologico,
                
                -- [TASK 12] Sanitização de Idade Sentinela (999 -> NULL)
                CASE 
                    WHEN TRY_CAST(PA_IDADE AS INTEGER) = 999 OR TRY_CAST(PA_IDADE AS INTEGER) < 0 THEN NULL 
                    ELSE TRY_CAST(PA_IDADE AS INTEGER) 
                END AS idade_paciente_anos,
                
                PA_MUNPCN AS codigo_municipio_residencia_paciente,
                
                -- [TASK 11] Derivação Territorial IBGE da UF de Residência
                CASE SUBSTRING(TRIM(CAST(PA_MUNPCN AS VARCHAR)), 1, 2)
                    WHEN '11' THEN 'RO' WHEN '12' THEN 'AC' WHEN '13' THEN 'AM' WHEN '14' THEN 'RR' WHEN '15' THEN 'PA' WHEN '16' THEN 'AP' WHEN '17' THEN 'TO'
                    WHEN '21' THEN 'MA' WHEN '22' THEN 'PI' WHEN '23' THEN 'CE' WHEN '24' THEN 'RN' WHEN '25' THEN 'PB' WHEN '26' THEN 'PE' WHEN '27' THEN 'AL' WHEN '28' THEN 'SE' WHEN '29' THEN 'BA'
                    WHEN '31' THEN 'MG' WHEN '32' THEN 'ES' WHEN '33' THEN 'RJ' WHEN '35' THEN 'SP'
                    WHEN '41' THEN 'PR' WHEN '42' THEN 'SC' WHEN '43' THEN 'RS'
                    WHEN '50' THEN 'MS' WHEN '51' THEN 'MT' WHEN '52' THEN 'GO' WHEN '53' THEN 'DF'
                    ELSE uf
                END AS uf_residencia_paciente,
                
                TRY_CAST(PA_QTDPRO AS BIGINT) AS quantidade_produzida,
                TRY_CAST(PA_QTDAPR AS BIGINT) AS quantidade_aprovada,
                TRY_CAST(PA_VALPRO AS DOUBLE) AS valor_produzido_brl,
                COALESCE(TRY_CAST(PA_VALAPR AS DOUBLE), 0.0) AS valor_aprovado_brl,

                -- [CORRECAO-11] Flags de Integridade Contabil SIA
                -- flag_glosa_sia: QTDAPR=0 mas VALAPR>0 — glosa de quantidade silenciosa.
                --   Indica que o procedimento foi aprovado financeiramente mas a quantidade
                --   ficou zerada (erro de codificacao ou inconsistencia de processamento DATASUS).
                CASE
                    WHEN COALESCE(TRY_CAST(PA_QTDAPR AS BIGINT), 0) = 0
                     AND COALESCE(TRY_CAST(PA_VALAPR AS DOUBLE), 0.0) > 0.0
                    THEN TRUE
                    ELSE FALSE
                END AS flag_glosa_sia,

                -- flag_pab_tarifa_zero: quantidade aprovada > 0 mas valor_aprovado = 0.
                --   Detecta atendimentos computados sem remuneracao — comum em erros de
                --   tabela SIGTAP desatualizada ou procedimentos PAB mal parametrizados.
                CASE
                    WHEN COALESCE(TRY_CAST(PA_QTDAPR AS BIGINT), 0) > 0
                     AND COALESCE(TRY_CAST(PA_VALAPR AS DOUBLE), 0.0) = 0.0
                    THEN TRUE
                    ELSE FALSE
                END AS flag_pab_tarifa_zero,

                ano, mes, uf, id_execucao
            FROM delta_scan('{sia_bronze_delta}')
            WHERE uf = '{uf}'
            """
            try:
                arr_uf = self.engine.fetch_arrow(sql_uf)
                if len(arr_uf) > 0:
                    # [CORRECAO-16] Dynamic Partition Overwrite por UF.
                    # ANTES: mode = "overwrite" if idx == 0 else "append"
                    #   -> Em retries iniciados de qualquer UF != RO, as UFs anteriores
                    #      eram re-appendadas sem overwrite, gerando duplicatas.
                    # DEPOIS: mode="overwrite" + predicate por UF em toda iteracao.
                    #   -> Cada UF e sobrescrita de forma atomica e independente da ordem,
                    #      garantindo idempotencia estrita mesmo em retries parciais.
                    self._persist_silver_table(
                        "fct_atendimentos_ambulatoriais",
                        arr_uf,
                        execution_id,
                        f"bronze_sia_{uf}",
                        partition_by=["ano", "mes", "uf"],
                        mode="overwrite",
                        predicate=f"uf = '{uf}'",
                    )
                    total_sia_transformed += len(arr_uf)
                del arr_uf
            except (OSError, IOError) as e:
                # Erros de I/O em uma UF sao fatais para a UF — re-raise para o orquestrador
                logger.error(
                    f"[FATAL I/O] Falha critica ao processar SIA-{uf}: {e}",
                    exc_info=True,
                )
                raise
            except Exception as e:
                # [CORRECAO-17] Erros de schema/query: logar com stack trace e continuar
                # para nao bloquear as UFs restantes. O orquestrador contabiliza como
                # particao degradada via failed_partitions.
                logger.error(
                    f"[ERRO] Falha ao transformar SIA-{uf}: {e}",
                    exc_info=True,
                )

        logger.info(f"[SILVER SIA TOTAL] {total_sia_transformed:,} linhas gravadas em fct_atendimentos_ambulatoriais.")

    def transformar_glosas_hospitalares_para_silver(self, execution_id: str = "exec_glosas"):
        """
        Transforma e correlaciona as AIHs Rejeitadas (RJ) com as Críticas de Processamento (ER),
        materializando a tabela fct_glosas_hospitalares na camada Silver.
        """
        sih_rj_delta = os.path.join(self.bronze_path, "datasus", "sih_rj").replace("\\", "/")
        sih_er_delta = os.path.join(self.bronze_path, "datasus", "sih_er").replace("\\", "/")
        
        if not os.path.exists(sih_rj_delta):
            logger.warning("Bronze SIH-RJ não encontrado para transformação de glosas hospitalares.")
            return

        has_er = os.path.exists(sih_er_delta)
        er_source = f"delta_scan('{sih_er_delta}')" if has_er else "(SELECT NULL AS N_AIH, NULL AS CO_ERRO, NULL AS DS_ERRO, NULL AS ano, NULL AS mes, NULL AS uf WHERE 1=0)"

        sql_glosas = f"""
        SELECT
            -- [CORRECAO-06b] PK Deterministica de Glosas Hospitalares
            -- ANTES: md5(..., ROW_NUMBER() OVER ()) -- nao deterministico entre execucoes.
            -- DEPOIS: sha256 sobre N_AIH + codigo_erro + competencia (ano/mes/uf).
            sha256(concat_ws('-',
                COALESCE(CAST(r.N_AIH    AS VARCHAR), ''),
                COALESCE(CAST(e.CO_ERRO  AS VARCHAR), '000'),
                COALESCE(CAST(r.CNES     AS VARCHAR), ''),
                COALESCE(CAST(r.PROC_REA AS VARCHAR), ''),
                COALESCE(CAST(r.ano      AS VARCHAR), ''),
                COALESCE(CAST(r.mes      AS VARCHAR), ''),
                COALESCE(CAST(r.uf       AS VARCHAR), '')
            )) AS id_glosa_hospitalar,

            r.N_AIH AS numero_aih,
            r.CNES AS codigo_estabelecimento_cnes,
            r.MUNIC_MOV AS codigo_municipio_hospital,
            r.PROC_REA AS codigo_procedimento,
            TRY_CAST(r.VAL_TOT AS DOUBLE) AS valor_glosado_brl,
            COALESCE(TRY_CAST(e.CO_ERRO AS VARCHAR), 'NAO_INFORMADO') AS codigo_motivo_glosa,
            CASE 
                WHEN e.CO_ERRO IS NULL THEN 'Motivo de Rejeição Não Especificado'
                WHEN TRIM(CAST(e.CO_ERRO AS VARCHAR)) IN ('101', '060060', '060065') THEN 'Inconsistência cadastral ou incompatibilidade clínica'
                WHEN TRIM(CAST(e.CO_ERRO AS VARCHAR)) IN ('204', '060072') THEN 'Incompatibilidade de procedimento ou teto orçamentário'
                ELSE concat('Crítica de Processamento DATASUS (Código ', TRIM(CAST(e.CO_ERRO AS VARCHAR)), ')')
            END AS descricao_motivo_glosa,
            'SIH_REJEICAO_SUS' AS tipo_origem_glosa,
            r.ano, r.mes, r.uf,
            '{execution_id}' AS id_execucao
        FROM delta_scan('{sih_rj_delta}') r
        LEFT JOIN {er_source} e 
            ON r.N_AIH = e.N_AIH AND r.ano = e.ano AND r.mes = e.mes AND r.uf = e.uf
        """
        try:
            arr_glosas = self.engine.fetch_arrow(sql_glosas)
            self._persist_silver_table(
                "fct_glosas_hospitalares", 
                arr_glosas, 
                execution_id, 
                "bronze_sih_rj_er", 
                partition_by=["ano", "mes", "uf"],
                mode="overwrite"
            )
            del arr_glosas
        except Exception as e:
            # [CORRECAO-17] Glosas sao dados de auditoria — falha silenciosa e inaceitavel.
            logger.error(
                f"[ERRO] Falha na geracao de fct_glosas_hospitalares: {e}",
                exc_info=True,
            )
            raise
