"""
Silver Writer - QIMED Lakehouse V3.
Gerencia a persistência idempotente das tabelas canônicas da Camada Silver em formato Delta Lake.
Garante desduplicação por chave primária em reexecuções.
"""
import os
from typing import Any, Dict, List, Optional
import pandas as pd
from deltalake.writer import write_deltalake

from src.silver.mappers.base_mapper import CanonicalDataset
from src.utils.logging_config import setup_logger
from src.utils.config_loader import load_pipeline_config

logger = setup_logger(__name__)


class SilverWriter:
    """
    Gerencia as tabelas Delta da Camada Silver com garantia de idempotência.
    """

    SILVER_TABLES = [
        "dim_paciente",
        "dim_estabelecimento",
        "dim_municipio",
        "dim_tempo",
        "dim_procedimento",
        "dim_diagnostico",
        "dim_especialidade",
        "dim_gestor",
        "dim_operadoras_saude",
        "fct_internacao",
        "fct_atendimentos_ambulatoriais",
        "fct_procedimento",
        "fct_diagnostico",
        "fct_regulacao_filas",
        "fct_ressarcimento_sus",
        "fct_glosas_hospitalares",
    ]

    PRIMARY_KEYS = {
        # [CORRECAO-12] Alinhado ao schema canônico real de CanonicalTransformations.
        # ANTES: mistura de nomes em inglês (dim_patients, fct_encounters) e chaves
        #        inexistentes (patient_master_id, nr_abi, cd_operadora) que nunca
        #        correspondiam às colunas geradas pelo pipeline real.
        # DEPOIS: nomes e chaves exatamente como gerados por transformations.py e patient_identity.py.
        "dim_paciente":                   ["pseudonimo_paciente"],
        "dim_estabelecimento":            ["codigo_estabelecimento_cnes"],
        "dim_operadoras_saude":           ["codigo_registro_ans"],
        "dim_tempo":                      ["data"],
        "fct_internacao":                 ["id_internacao_hospitalar"],
        "fct_atendimentos_ambulatoriais": ["id_atendimento_ambulatorial"],
        "fct_glosas_hospitalares":        ["id_glosa_hospitalar"],
        "fct_ressarcimento_sus":          ["identificador_cobranca_abi"],
    }

    def __init__(self, silver_path: Optional[str] = None, silver_base_path: Optional[str] = None):
        cfg = load_pipeline_config()
        self.silver_path = silver_path or silver_base_path or cfg.get("paths", {}).get("silver_dir", "lakehouse/silver")
        os.makedirs(self.silver_path, exist_ok=True)

    def get_table_path(self, table_name: str) -> str:
        return os.path.join(self.silver_path, table_name)

    def _write_table(
        self,
        df: pd.DataFrame,
        table_name: str,
        mode: str = "overwrite",
        schema_mode: str = "merge",
        partition_cols: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Escreve um DataFrame na tabela Delta Silver de forma idempotente.

        [CORRECAO-04] Elimina DeltaTable.to_pandas() + pd.concat() + drop_duplicates()
        que carregava a tabela inteira na RAM a cada execucao.

        Estrategia:
        - Se `partition_cols` for fornecido e existirem no DataFrame, usa
          Dynamic Partition Overwrite (predicate por valor de cada particao),
          substituindo apenas a fatia afetada pelos dados novos.
        - Caso contrario, sobrescreve a tabela inteira (mesmo custo de I/O,
          mas sem custo de memoria proporcional ao historico acumulado).
        - A deduplicacao por PK e feita sobre o DataFrame de entrada antes
          de gravar, sem ler dados historicos do Delta em memoria.
        """
        if df is None or df.empty:
            return {"status": "skipped_empty", "rows_written": 0}

        target_path = self.get_table_path(table_name)

        # [V2-03] Deduplicacao apenas sobre os dados de entrada — sem fallback de PKs legadas.
        # Tabelas sem chave no dicionario PRIMARY_KEYS simplesmente nao sao deduplicadas,
        # tornando o comportamento explicito em vez de silencioso.
        pk_candidates = self.PRIMARY_KEYS.get(table_name, [])
        pk_col = next((c for c in pk_candidates if c in df.columns), None)
        if pk_col:
            df = df.drop_duplicates(subset=[pk_col], keep="last")

        # Inferir predicado de particao a partir dos valores unicos no DataFrame de entrada.
        predicate: Optional[str] = None
        active_partition_cols = [c for c in (partition_cols or []) if c in df.columns]
        if active_partition_cols:
            # Dynamic Partition Overwrite: sobrescreve apenas as particoes
            # presentes nos dados de entrada, preservando as demais intactas.
            clauses = []
            for col in active_partition_cols:
                vals = df[col].dropna().unique().tolist()
                if len(vals) == 1:
                    v = vals[0]
                    clauses.append(f"{col} = '{v}'" if isinstance(v, str) else f"{col} = {v}")
                elif len(vals) > 1:
                    quoted = ", ".join(f"'{v}'" if isinstance(v, str) else str(v) for v in vals)
                    clauses.append(f"{col} IN ({quoted})")
            if clauses:
                predicate = " AND ".join(clauses)

        # [V2-01] Eliminado o bloco DeltaTable.to_pandas() + pd.concat() que carregava
        # o historico inteiro na RAM. O predicate + mode="overwrite" garante idempotencia
        # sem precisar ler dados historicos. Para dimensoes sem particao, o overwrite
        # completo tem o mesmo custo de I/O porem sem custo de RAM proporcional ao historico.

        import pyarrow as pa
        arrow_table = pa.Table.from_pandas(df, preserve_index=False)

        write_kwargs: Dict[str, Any] = dict(
            mode="overwrite" if predicate else mode,
            schema_mode=schema_mode,
            partition_by=active_partition_cols or None,
        )
        if predicate:
            write_kwargs["predicate"] = predicate

        write_deltalake(target_path, arrow_table, **write_kwargs)
        return {"status": "success", "rows_written": len(df)}

    def write_canonical_dataset(self, dataset: CanonicalDataset) -> Dict[str, Any]:
        """
        Persiste todas as entidades de um CanonicalDataset em suas respectivas tabelas Silver com idempotência.
        """
        return {
            "dim_patients": self._write_table(dataset.dim_patients, "dim_patients"),
            "dim_organizations": self._write_table(dataset.dim_organizations, "dim_organizations"),
            "dim_health_plans": self._write_table(dataset.dim_health_plans, "dim_health_plans"),
            "fct_encounters": self._write_table(dataset.fct_encounters, "fct_encounters"),
            "fct_conditions": self._write_table(dataset.fct_conditions, "fct_conditions"),
            "fct_procedures": self._write_table(dataset.fct_procedures, "fct_procedures"),
            "fct_referrals": self._write_table(dataset.fct_referrals, "fct_referrals"),
        }
