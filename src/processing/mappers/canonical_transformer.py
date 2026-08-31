"""
Canonical Transformer - Camada Silver (Canônica) - QIMED Lakehouse.
Aplica De-Para declarativo, sanitização de valores sentinelas (Idades 999, CIDs inválidos),
computação determinística de chaves SHA-256 e Master Patient Index (MPI) via DuckDB.
"""
import os
from typing import Any, Dict, Optional, Union
import duckdb
import pyarrow as pa

from src.processing.mappers.schema_registry import SchemaRegistry
from src.utils.logging_config import setup_logger

logger = setup_logger(__name__)


class CanonicalTransformer:
    """
    Motor de transformação canônica para fct_internacao da camada Silver.
    """

    def __init__(self, mpi_salt: Optional[str] = None):
        self.salt = mpi_salt or os.getenv("QIMED_MPI_SALT", "qimed_mpi_salt_v3_2026")
        self.registry = SchemaRegistry()

    def transform_to_fct_internacao(
        self,
        raw_table: Union[pa.Table, str],
        source_format: str = "generic_csv"
    ) -> pa.Table:
        """
        Executa a transformação out-of-core via DuckDB a partir dos dados brutos (Bronze).
        Retorna uma Apache Arrow Table canônica tipada e sanitizada.
        """
        mapping = self.registry.get_mapping(source_format)
        con = duckdb.connect(":memory:")

        if isinstance(raw_table, pa.Table):
            con.register("raw_source", raw_table)
        elif isinstance(raw_table, str):
            safe_path = raw_table.replace("\\", "/")
            if safe_path.endswith(".parquet") or safe_path.endswith(".pq"):
                con.execute(f"CREATE VIEW raw_source AS SELECT * FROM read_parquet('{safe_path}');")
            else:
                con.execute(f"CREATE VIEW raw_source AS SELECT * FROM read_csv_auto('{safe_path}');")
        else:
            raise ValueError(f"Formato de entrada não suportado: {type(raw_table)}")

        columns_info = con.execute("DESCRIBE raw_source;").fetchall()
        source_columns = {col[0].upper(): col[0] for col in columns_info}

        active_mapping: Dict[str, str] = {}
        for src_col, canon_col in mapping.items():
            if src_col.upper() in source_columns:
                real_src_col = source_columns[src_col.upper()]
                active_mapping[canon_col] = f'"{real_src_col}"'

        def get_col(canonical_name: str, default_expr: str = "NULL") -> str:
            return active_mapping.get(canonical_name, default_expr)

        transform_sql = f"""
        WITH mapped AS (
            SELECT
                {get_col('numero_aih')} AS raw_numero_aih,
                {get_col('codigo_estabelecimento_cnes')} AS raw_cnes,
                {get_col('codigo_municipio_residencia_paciente')} AS raw_munic_res,
                {get_col('codigo_municipio_hospital')} AS raw_munic_hosp,
                {get_col('data_nascimento_paciente')} AS raw_nasc,
                {get_col('idade_anos')} AS raw_idade,
                {get_col('sexo_biologico')} AS raw_sexo,
                {get_col('data_internacao')} AS raw_dt_inter,
                {get_col('data_alta')} AS raw_dt_alta,
                {get_col('codigo_cid10_principal')} AS raw_cid_princ,
                {get_col('codigo_cid10_secundario')} AS raw_cid_secun,
                {get_col('dias_permanencia_real')} AS raw_dias_perm,
                {get_col('indicador_obito')} AS raw_obito,
                {get_col('valor_total_brl')} AS raw_val_tot,
                {get_col('valor_uti_brl')} AS raw_val_uti,
                {get_col('codigo_procedimento_realizado')} AS raw_proc_rea,
                {get_col('cpf_paciente')} AS raw_cpf
            FROM raw_source
        ),
        sanitized AS (
            SELECT
                TRY_CAST(raw_numero_aih AS VARCHAR) AS numero_aih,
                TRY_CAST(raw_cnes AS VARCHAR) AS codigo_estabelecimento_cnes,
                TRY_CAST(raw_munic_res AS VARCHAR) AS codigo_municipio_residencia_paciente,
                TRY_CAST(raw_munic_hosp AS VARCHAR) AS codigo_municipio_hospital,
                TRY_CAST(raw_nasc AS VARCHAR) AS data_nascimento_paciente,
                TRY_CAST(raw_dt_inter AS VARCHAR) AS data_internacao,
                TRY_CAST(raw_dt_alta AS VARCHAR) AS data_alta,
                TRY_CAST(raw_proc_rea AS VARCHAR) AS codigo_procedimento_realizado,
                TRY_CAST(raw_cpf AS VARCHAR) AS cpf_paciente,

                -- 1. Sanitização de Sentinelas de Idade (idades >= 130, < 0 ou 999 -> NULL)
                CASE
                    WHEN TRY_CAST(raw_idade AS INTEGER) >= 130 
                      OR TRY_CAST(raw_idade AS INTEGER) < 0 
                      OR TRY_CAST(raw_idade AS INTEGER) = 999 
                    THEN NULL
                    ELSE TRY_CAST(raw_idade AS INTEGER)
                END AS idade_anos,

                -- 2. Sanitização de Sexo Biológico (M, F ou I)
                CASE
                    WHEN UPPER(TRIM(TRY_CAST(raw_sexo AS VARCHAR))) IN ('M', '1', 'MASCULINO', 'MASC') THEN 'M'
                    WHEN UPPER(TRIM(TRY_CAST(raw_sexo AS VARCHAR))) IN ('F', '2', 'FEMININO', 'FEM') THEN 'F'
                    ELSE 'I'
                END AS sexo_biologico,

                -- 3. Sanitização de Sentinelas de CID-10 ('0000', '000', '0', '9999', 'NULL', 'NA' -> NULL)
                CASE
                    WHEN UPPER(TRIM(TRY_CAST(raw_cid_princ AS VARCHAR))) IN ('0000', '000', '0', '9999', 'NULL', 'NA', '') THEN NULL
                    ELSE UPPER(TRIM(TRY_CAST(raw_cid_princ AS VARCHAR)))
                END AS codigo_cid10_principal,

                CASE
                    WHEN UPPER(TRIM(TRY_CAST(raw_cid_secun AS VARCHAR))) IN ('0000', '000', '0', '9999', 'NULL', 'NA', '') THEN NULL
                    ELSE UPPER(TRIM(TRY_CAST(raw_cid_secun AS VARCHAR)))
                END AS codigo_cid10_secundario,

                -- 4. Sanitização de Indicador de Óbito (0 ou 1)
                CASE
                    WHEN UPPER(TRIM(TRY_CAST(raw_obito AS VARCHAR))) IN ('1', 'S', 'SIM', 'TRUE', 'T', 'O') THEN 1
                    ELSE 0
                END AS indicador_obito,

                -- 5. Tipagem e Coalesce de Valores Numéricos
                COALESCE(TRY_CAST(raw_dias_perm AS INTEGER), 0) AS dias_permanencia_real,
                COALESCE(TRY_CAST(raw_val_tot AS DOUBLE), 0.0) AS valor_total_brl,
                COALESCE(TRY_CAST(raw_val_uti AS DOUBLE), 0.0) AS valor_uti_brl
            FROM mapped
        )
        SELECT
            -- Chave de Atendimento SHA-256 (Determinística)
            sha256(
                COALESCE(numero_aih, 'NA') || '_' || 
                COALESCE(codigo_estabelecimento_cnes, 'NA')
            ) AS id_atendimento,

            -- Master Patient Index (MPI) - Pseudonimização SHA-256 com Salt
            sha256(
                COALESCE(cpf_paciente, '') || '_' ||
                COALESCE(data_nascimento_paciente, '') || '_' ||
                COALESCE(sexo_biologico, '') || '_' ||
                COALESCE(codigo_municipio_residencia_paciente, '') || '_' ||
                '{self.salt}'
            ) AS id_paciente,

            -- Identificador de Registro Forense (Linha Física Imutável)
            sha256(
                COALESCE(numero_aih, '') || '_' ||
                COALESCE(codigo_estabelecimento_cnes, '') || '_' ||
                COALESCE(data_internacao, '') || '_' ||
                COALESCE(CAST(valor_total_brl AS VARCHAR), '') || '_' ||
                COALESCE(codigo_cid10_principal, '')
            ) AS id_registro,

            numero_aih,
            codigo_estabelecimento_cnes,
            codigo_municipio_residencia_paciente,
            codigo_municipio_hospital,
            data_nascimento_paciente,
            idade_anos,
            sexo_biologico,
            data_internacao,
            data_alta,
            codigo_cid10_principal,
            codigo_cid10_secundario,
            dias_permanencia_real,
            indicador_obito,
            valor_total_brl,
            valor_uti_brl,
            codigo_procedimento_realizado,
            cpf_paciente
        FROM sanitized;
        """

        arrow_res = con.execute(transform_sql).arrow()
        if hasattr(arrow_res, "read_all"):
            canonical_table = arrow_res.read_all()
        else:
            canonical_table = arrow_res

        logger.info(
            f"[CANONICAL_TRANSFORMER] Sucesso: {canonical_table.num_rows} linhas transformadas "
            f"para fct_internacao (formato: {source_format})."
        )
        return canonical_table
