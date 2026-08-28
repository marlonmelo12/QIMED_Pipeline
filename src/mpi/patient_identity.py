"""
Master Patient Index (MPI) & Identidade Clinica Vetorizada - QIMED Lakehouse V3.
Implementa a derivacao deterministica de identidades em 4 niveis semanticos:
1. Identificador de Atendimento (AIH / APAC)
2. Identificador de Registro (hash deterministico forense da linha fisica - SEM row_number)
3. Pseudonimo do Paciente (LGPD - SHA-256 com salt obrigatorio via env var)
4. Identificador de Paciente Candidato (MPI Heuristico - SHA-256)
100% vetorizado em SQL/Arrow sem loops Python ou iterrows().

SEGURANCA LGPD:
  O salt de pseudonimizacao DEVE ser configurado via variavel de ambiente QIMED_MPI_SALT.
  O pipeline recusa execucao se a variavel estiver ausente (fail-fast).
  Gerar um salt seguro: openssl rand -hex 32
"""
import os
import hashlib
from typing import Any, Dict, List, Optional
import pyarrow as pa

from src.processing.duckdb_engine import DuckDBEngine
from src.utils.logging_config import setup_logger
from src.utils.config_loader import load_pipeline_config

logger = setup_logger(__name__)


class PatientIdentityResolver:
    """
    Resolvedor vetorizado de identidades e pseudonimiza??o em conformidade com a LGPD.
    """

    def __init__(self, duck_engine: Optional[DuckDBEngine] = None, config: Optional[Dict[str, Any]] = None):
        self.cfg = config or load_pipeline_config()
        self.engine = duck_engine or DuckDBEngine(config=self.cfg)
        mpi_cfg = self.cfg.get("mpi", {})
        salt_env = mpi_cfg.get("salt_secret_env", "QIMED_MPI_SALT")

        # [CORRECAO-09] Fail-fast obrigatorio: sem salt configurado, o pipeline nao executa.
        # Isso previne que qualquer ambiente rode sem protecao LGPD ativa.
        # ANTES: self.salt = os.getenv(salt_env, mpi_cfg.get("default_salt", "qimed_mpi_salt_v3_2026"))
        self.salt = os.getenv(salt_env)
        if not self.salt:
            raise RuntimeError(
                f"\n{'='*72}\n"
                f"[ERRO CRITICO - LGPD] A variavel de ambiente '{salt_env}' nao esta configurada.\n"
                f"O pipeline recusa execucao sem salt criptografico para proteger dados de pacientes.\n"
                f"\nComo gerar e configurar um salt seguro:\n"
                f"  Linux/Mac : export {salt_env}=$(openssl rand -hex 32)\n"
                f"  Windows   : set {salt_env}=<valor gerado por openssl rand -hex 32>\n"
                f"  Docker    : Adicionar '{salt_env}=<valor>' no arquivo .env\n"
                f"\nArquive o salt em um secrets manager (Vault, AWS Secrets, etc.).\n"
                f"{'='*72}"
            )

        logger.info(
            f"[MPI] Salt LGPD carregado da variavel '{salt_env}' "
            f"(SHA-256, {len(self.salt)} chars). Pseudonimizacao ativa."
        )


    def resolve_identities_sql(self, source_relation_sql: str) -> str:
        """
        Gera a expressao SQL vetorizada que projeta os 4 niveis de identidade clinica.

        NIVEL 1 - identificador_atendimento      : Chave de negocio (AIH / APAC)
        NIVEL 2 - identificador_registro         : SHA-256 deterministico (SEM row_number)
        NIVEL 3 - pseudonimo_paciente            : SHA-256 com salt LGPD obrigatorio
        NIVEL 4 - identificador_paciente_candidato: MPI heuristico (SHA-256 + salt)
        """
        return f"""
        SELECT
            *,
            -- Nivel 1: Identificador de Atendimento (chave de negocio natural)
            COALESCE(
                TRY_CAST(numero_aih AS VARCHAR),
                TRY_CAST(numero_documento_autorizacao AS VARCHAR),
                'ATEND_NA'
            ) AS identificador_atendimento,

            -- [CORRECAO-08] Nivel 2: Identificador de Registro (Hash Deterministico Forense)
            -- ANTES: md5(... || row_number() OVER ()) -- nao deterministico entre execucoes.
            -- DEPOIS: sha256 sobre campos estaveis de negocio da linha fisica.
            sha256(
                COALESCE(TRY_CAST(numero_aih                           AS VARCHAR), '') || '_' ||
                COALESCE(TRY_CAST(numero_documento_autorizacao         AS VARCHAR), '') || '_' ||
                COALESCE(TRY_CAST(codigo_estabelecimento_cnes          AS VARCHAR), '') || '_' ||
                COALESCE(TRY_CAST(data_internacao                      AS VARCHAR), '') || '_' ||
                COALESCE(TRY_CAST(data_nascimento_paciente             AS VARCHAR), '') || '_' ||
                COALESCE(TRY_CAST(sexo_biologico                       AS VARCHAR), '') || '_' ||
                COALESCE(TRY_CAST(codigo_municipio_residencia_paciente AS VARCHAR), '')
            ) AS identificador_registro,

            -- [CORRECAO-09] Nivel 3: Pseudonimo do Paciente (SHA-256 + salt LGPD obrigatorio)
            -- ANTES: md5(campos || salt_hardcoded) -- MD5 fraco e salt publico no repo.
            -- DEPOIS: sha256(campos || salt_de_env_var).
            -- Salvaguarda neonatal: quando nasc == internacao, inclui CNES + AIH
            -- para distinguir gemeos e partos multiplos no mesmo dia/unidade.
            CASE
                WHEN data_nascimento_paciente IS NOT NULL
                 AND data_nascimento_paciente = data_internacao THEN
                    sha256(
                        COALESCE(TRY_CAST(data_nascimento_paciente            AS VARCHAR), '') || '_' ||
                        COALESCE(TRY_CAST(sexo_biologico                      AS VARCHAR), '') || '_' ||
                        COALESCE(TRY_CAST(codigo_municipio_residencia_paciente AS VARCHAR), '') || '_' ||
                        COALESCE(TRY_CAST(codigo_estabelecimento_cnes         AS VARCHAR), '') || '_' ||
                        COALESCE(TRY_CAST(numero_aih                          AS VARCHAR), '') ||
                        '_{self.salt}_neo'
                    )
                ELSE
                    sha256(
                        COALESCE(TRY_CAST(data_nascimento_paciente            AS VARCHAR), '') || '_' ||
                        COALESCE(TRY_CAST(sexo_biologico                      AS VARCHAR), '') || '_' ||
                        COALESCE(TRY_CAST(codigo_municipio_residencia_paciente AS VARCHAR), '') ||
                        '_{self.salt}'
                    )
            END AS pseudonimo_paciente,

            -- [CORRECAO-09] Nivel 4: Identificador de Paciente Candidato (MPI Heuristico SHA-256)
            -- ANTES: md5 com salt hardcoded.
            -- DEPOIS: sha256 com salt de env var.
            CASE
                WHEN data_nascimento_paciente IS NOT NULL
                 AND data_nascimento_paciente = data_internacao THEN
                    'pac_' || substring(sha256(
                        COALESCE(TRY_CAST(data_nascimento_paciente            AS VARCHAR), '') || '_' ||
                        COALESCE(TRY_CAST(codigo_municipio_residencia_paciente AS VARCHAR), '') || '_' ||
                        COALESCE(TRY_CAST(codigo_estabelecimento_cnes         AS VARCHAR), '') || '_' ||
                        COALESCE(TRY_CAST(numero_aih                          AS VARCHAR), '') ||
                        '_{self.salt}_neo_mpi'
                    ), 1, 16)
                ELSE
                    'pac_' || substring(sha256(
                        COALESCE(TRY_CAST(data_nascimento_paciente            AS VARCHAR), '') || '_' ||
                        COALESCE(TRY_CAST(codigo_municipio_residencia_paciente AS VARCHAR), '') ||
                        '_{self.salt}_mpi'
                    ), 1, 16)
            END AS identificador_paciente_candidato
        FROM ({source_relation_sql})
        """

    def apply_vectorized_resolution(self, table_name: str) -> pa.Table:
        """
        Aplica a resolu??o de MPI vetorizada sobre uma tabela j? carregada no DuckDB.
        """
        query_sql = self.resolve_identities_sql(f"SELECT * FROM {table_name}")
        return self.engine.fetch_arrow(query_sql)
