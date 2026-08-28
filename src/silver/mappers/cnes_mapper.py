"""
Mapper Semântico para o DATASUS CNES (Cadastro Nacional de Estabelecimentos de Saúde).
Transforma registros de estabelecimentos de saúde no modelo canônico dim_organizations.
"""
from datetime import datetime
from typing import Dict, Any, Optional
import pandas as pd

from src.silver.mappers.base_mapper import BaseSemanticMapper, CanonicalDataset
from src.silver.terminology import TerminologyService
from src.utils.logging_config import get_logger

logger = get_logger(__name__)


class CnesSemanticMapper(BaseSemanticMapper):
    """
    Transforma dados de estabelecimentos de saúde do CNES na dimensão dim_organizations.
    """

    def map_to_canonical(self, df: pd.DataFrame, source_metadata: Optional[Dict[str, Any]] = None) -> CanonicalDataset:
        if df.empty:
            return CanonicalDataset()

        logger.info(f"Mapeando {len(df)} registros do CNES para o modelo canônico dim_organizations")
        src_meta = source_metadata or {}
        source_file = src_meta.get("source_file", "datasus_cnes")

        orgs_list = []

        for _, row in df.iterrows():
            cnes_raw = str(row.get("CNES", "")).strip()
            if not cnes_raw or cnes_raw in ("nan", "None", ""):
                continue

            cnes_clean = "".join(filter(str.isdigit, cnes_raw)).zfill(7)
            org_id = f"org_cnes_{cnes_clean}"

            munic_code = str(row.get("CODUFMUN", row.get("MUNIC_RES", ""))).strip()
            norm_ibge, ibge_meta = TerminologyService.normalize_ibge_municipality(munic_code)

            name = str(row.get("NOME_FANTASIA", row.get("RAZAO_SOCIAL", f"Estabelecimento CNES {cnes_clean}"))).strip()
            unit_type = str(row.get("TP_UNID", row.get("TIPO_UNIDADE", "Hospital"))).strip()

            beds = row.get("LEITOS", row.get("QTLEITOS", None))
            try:
                bed_count = int(float(beds)) if pd.notna(beds) else None
            except Exception:
                bed_count = None

            orgs_list.append({
                "organization_id": org_id,
                "cnes_code": cnes_clean,
                "name": name,
                "facility_type": unit_type,
                "municipality_code": norm_ibge,
                "state": ibge_meta.get("uf_abbreviation") if ibge_meta else None,
                "bed_capacity": bed_count,
                "_source_file": source_file,
                "_updated_at": datetime.utcnow().isoformat()
            })

        dim_organizations = pd.DataFrame(orgs_list).drop_duplicates(subset=["organization_id"])

        canonical = CanonicalDataset(
            dim_organizations=dim_organizations,
            metadata={"source": "datasus_cnes", "row_count": len(df)}
        )
        logger.info(f"Mapeadas {len(dim_organizations)} organizações canônicas")
        return canonical
