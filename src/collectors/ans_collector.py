"""
Coletor de Dados da ANS (Agencia Nacional de Saude Suplementar).
Coleta dados abertos de operadoras, beneficiarios por municipio,
ressarcimento ao SUS (ABI/FNS) e notificacoes de negativas de cobertura (NIP).

Fontes:
  - Cadop (Cadastro de Operadoras): https://dados.ans.gov.br/Acesso_as_informacoes/operadoras/
  - Beneficiarios por Municipio (SIB): https://www.ans.gov.br/anstabnet/tabnet/
  - Ressarcimento ao SUS (ABI/FNS): https://dados.ans.gov.br/Acesso_as_informacoes/dados_abertos/
  - NIP (Notificacoes de Negativa): https://dados.ans.gov.br/Acesso_as_informacoes/dados_abertos/
"""
import io
import os
import re
import zipfile
import requests
import pandas as pd
from typing import Any, Dict, List, Optional
from src.collectors.base import BaseCollector, CollectorConfig
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

MOJIBAKE_REPLACEMENTS = {
    "Ã\x8d": "Í", "Ã\x93": "Ó", "Ã\x9a": "Ú", "Ã\x87": "Ç", "Ã\x83": "Ã", "Ã\x95": "Õ",
    "Ã\x94": "Ô", "Ã\x89": "É", "Ã\x81": "Á", "Ã\x8a": "Ê", "Ã\x80": "À", "Ã\x82": "Â",
    "ÃŠ": "Ê", "Ãš": "Ú", "Ã‰": "É", "Ã“": "Ó", "ÃÁ": "Á", "Ã‡": "Ç", "Ãƒ": "Ã", "Ã‚": "Â",
    "Ã•": "Õ", "Ã”": "Ô", "Ã€": "À", "Ã\xcd": "Í", "Ã\xd3": "Ó", "Ã\xda": "Ú", "Ã\xc7": "Ç",
    "Ã\xc3": "Ã", "Ã\xd5": "Õ", "Ã\xd4": "Ô", "Ã\xc9": "É", "Ã\xc1": "Á", "Ã\xca": "Ê", "Ã\xc0": "À",
    "Ã­": "í", "Ã\xad": "í", "Ã³": "ó", "Ãº": "ú", "Ã§": "ç", "Ã£": "ã", "Ãµ": "õ",
    "Ã´": "ô", "Ã©": "é", "Ã¡": "á", "Ãª": "ê", "Ã ": "à", "Ã¢": "â",
    "â€“": "-", "â€”": "-", "â€": '"', "Â°": "°", "Âº": "º", "Âª": "ª",
    "ÂO": "ÃO", "Âo": "ão", "MOURÂO": "MOURÃO", "ÇAO": "ÇÃO",
    "SAÃDE": "SAÚDE", "saÃde": "saúde", "SaÃde": "Saúde",
}

ANS_BASE_PDA_URL = "https://dadosabertos.ans.gov.br/FTP/PDA/"
ANS_CADOP_URL = f"{ANS_BASE_PDA_URL}operadoras_de_plano_de_saude_ativas/Relatorio_cadop.csv"
ANS_BENEFICIARIOS_BASE_URL = f"{ANS_BASE_PDA_URL}taxa_de_cobertura_de_planos_de_saude-047/pda-047-taxa_cobertura.csv"
ANS_RESSARCIMENTO_URL = f"{ANS_BASE_PDA_URL}ressarcimento_ao_SUS_cobranca_arrecadacao/PDA_Cobranca_e_Arrecadacao_SUS.csv"
ANS_NIP_BASE_URL = f"{ANS_BASE_PDA_URL}demandas_dos_consumidores_nip/pda-013-demandas_dos_consumidores_nip-"


class AnsCollector(BaseCollector):
    """
    Coletor de dados de Saude Suplementar da ANS.
    Suporta quatro modalidades de coleta:
      - 'operadoras': Cadastro nacional de operadoras ativas (Cadop).
      - 'beneficiarios': Beneficiarios por municipio e UF (SIB).
      - 'ressarcimento': Ressarcimento ao SUS (ABI / FNS).
      - 'nip': Notificacoes de negativas de cobertura (NIP).
    """

    MODALIDADES_VALIDAS = ("operadoras", "beneficiarios", "ressarcimento", "nip")

    def __init__(
        self,
        modalidade: str = "operadoras",
        uf: str = "CE",
        year: int = 2026,
        month: int = 5,
        source_path_or_url: Optional[str] = None,
        config: Optional[CollectorConfig] = None,
    ):
        super().__init__(config=config)
        if modalidade not in self.MODALIDADES_VALIDAS:
            raise ValueError(
                f"Modalidade '{modalidade}' invalida. Use: {self.MODALIDADES_VALIDAS}"
            )
        self.modalidade = modalidade
        self.uf = uf.upper()
        self.year = year
        self.month = month
        self.source_path_or_url = source_path_or_url
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": "QIMED-Pipeline/1.0 dadosabertos.ans.gov.br"})

    def get_source_type(self) -> str:
        return f"ans_{self.modalidade}"

    def fetch(self) -> Any:
        if self.source_path_or_url and os.path.exists(self.source_path_or_url):
            logger.info(f"Carregando ANS ({self.modalidade}) de arquivo local: {self.source_path_or_url}")
            return self._ler_local(self.source_path_or_url)

        logger.info(f"Coletando ANS modalidade='{self.modalidade}' uf={self.uf} {self.year}/{self.month:02d}")
        dispatch = {
            "operadoras": self._fetch_operadoras,
            "beneficiarios": self._fetch_beneficiarios,
            "ressarcimento": self._fetch_ressarcimento,
            "nip": self._fetch_nip,
        }
        return dispatch[self.modalidade]()

    @staticmethod
    def _fix_text_mojibake(val: Any) -> Any:
        if val is None or pd.isna(val):
            return val
        s = str(val)
        if any(k in s for k in ("Ã", "Â", "â€")):
            # 1. Aplica decodificação reversa latin-1 -> utf-8 se for válido
            try:
                fixed = s.encode("latin-1").decode("utf-8")
                if not any(k in fixed for k in ("Ã\x8d", "Ã\x93", "Ã\x9a", "Ã\x87", "Ã\x83", "ÃŠ", "Ãš", "Ã‰", "Ã“", "Ã‡", "Ãƒ")):
                    s = fixed
            except Exception:
                pass

            # 2. Aplica dicionário determinístico de substituições
            for k, v in MOJIBAKE_REPLACEMENTS.items():
                if k in s:
                    s = s.replace(k, v)

            # 3. Tenta decodificação reversa novamente caso tenham sobrado resíduos
            try:
                if "Ã" in s and not any(w in s for w in ("ÃO", "ÃES", "ÃOS")):
                    fixed = s.encode("latin-1").decode("utf-8")
                    s = fixed
            except Exception:
                pass

        return re.sub(r"\s+", " ", s).strip()

    def _decode_content_safe(self, content: bytes) -> str:
        """Tenta decodificar em utf-8-sig / utf-8, com fallback para latin-1 / cp1252."""
        for enc in ("utf-8-sig", "utf-8"):
            try:
                decoded = content.decode(enc)
                if any(m in decoded for m in ("Ã\x8d", "Ã\x93", "Ã\x9a", "Ã\x87", "Ã\x83", "Ã\x95", "Ã\x94", "Ã\x89", "Ã\x81", "Ã­", "Ã³", "Ãº", "Ã§", "Ã£", "Ãµ", "Ã´", "Ã©", "Ã¡", "Ãª", "Ã ", "â€")):
                    try:
                        lat_decoded = content.decode("latin-1")
                        if not any(m in lat_decoded for m in ("Ã\x8d", "Ã\x93", "Ã\x9a", "Ã\x87", "Ã\x83", "Ã\x95", "Ã\x94", "Ã\x89", "Ã\x81", "Ã­", "Ã³", "Ãº", "Ã§", "Ã£", "Ãµ", "Ã´", "Ã©", "Ã¡", "Ãª", "Ã ")):
                            return lat_decoded
                    except Exception:
                        pass
                return decoded
            except (UnicodeDecodeError, UnicodeError):
                continue
        for enc in ("latin-1", "cp1252", "iso-8859-1"):
            try:
                return content.decode(enc)
            except Exception:
                continue
        return content.decode("latin-1", errors="replace")

    def _fetch_operadoras(self) -> pd.DataFrame:
        logger.info(f"Baixando Cadop de operadoras ativas: {ANS_CADOP_URL}")
        resp = self._session.get(ANS_CADOP_URL, timeout=60)
        resp.raise_for_status()
        df = pd.read_csv(
            io.StringIO(self._decode_content_safe(resp.content)), sep=";", dtype=str
        )
        logger.info(f"Cadop baixado com {len(df)} operadoras ativas.")
        return df

    def _fetch_beneficiarios(self) -> pd.DataFrame:
        logger.info(f"Baixando beneficiarios ANS: {ANS_BENEFICIARIOS_BASE_URL}")
        resp = self._session.get(ANS_BENEFICIARIOS_BASE_URL, timeout=60)
        resp.raise_for_status()
        df = pd.read_csv(io.StringIO(self._decode_content_safe(resp.content)), sep=";", dtype=str)
        logger.info(f"Beneficiarios ANS baixados: {len(df)} registros.")
        return df

    def _fetch_ressarcimento(self) -> pd.DataFrame:
        logger.info(f"Baixando ressarcimento ao SUS: {ANS_RESSARCIMENTO_URL}")
        resp = self._session.get(ANS_RESSARCIMENTO_URL, timeout=60)
        resp.raise_for_status()
        df = pd.read_csv(io.StringIO(self._decode_content_safe(resp.content)), sep=";", dtype=str)
        logger.info(f"Ressarcimento SUS baixado: {len(df)} registros.")
        return df

    def _fetch_nip(self) -> pd.DataFrame:
        last_exc = None
        for yr in [self.year, 2024, 2023]:
            url = f"{ANS_NIP_BASE_URL}{yr}.csv"
            try:
                logger.info(f"Baixando NIP (negativas de cobertura): {url}")
                resp = self._session.get(url, timeout=60)
                resp.raise_for_status()
                df = pd.read_csv(io.StringIO(self._decode_content_safe(resp.content)), sep=";", dtype=str)
                logger.info(f"NIP baixado: {len(df)} notificacoes de {yr}.")
                return df
            except Exception as exc:
                last_exc = exc
                logger.warning(f"Falha ao baixar NIP para {yr} ({exc}). Tentando ano anterior...")
        raise RuntimeError(f"Falha no download dos dados reais de NIP da ANS: {last_exc}")

    def parse(self, raw_data: Any) -> pd.DataFrame:
        dispatch = {
            "operadoras": self._parse_operadoras,
            "beneficiarios": self._parse_beneficiarios,
            "ressarcimento": self._parse_ressarcimento,
            "nip": self._parse_nip,
        }
        df = dispatch[self.modalidade](raw_data)
        logger.info(f"ANS {self.modalidade} parseado: {len(df)} registros, {len(df.columns)} colunas.")
        return df

    def _parse_operadoras(self, df: pd.DataFrame) -> pd.DataFrame:
        mapa = {
            "Registro_ANS": "cd_operadora", "REGISTRO_OPERADORA": "cd_operadora", "Registro_Operadora": "cd_operadora",
            "CNPJ": "cnpj",
            "Razao_Social": "razao_social", "RAZAO_SOCIAL": "razao_social",
            "Nome_Fantasia": "nome_fantasia", "NOME_FANTASIA": "nome_fantasia",
            "Modalidade": "modalidade", "MODALIDADE": "modalidade",
            "Municipio": "municipio", "CIDADE": "municipio", "Cidade": "municipio",
            "UF": "uf", "CEP": "cep",
            "Situacao": "situacao", "SITUACAO": "situacao",
            "Data_Registro_ANS": "dt_registro_ans", "DATA_REGISTRO_ANS": "dt_registro_ans",
        }
        df = df.rename(columns={k: v for k, v in mapa.items() if k in df.columns})
        df = df.loc[:, ~df.columns.duplicated()].copy()
        if "situacao" not in df.columns:
            df["situacao"] = "ATIVA"
        for col in ["cd_operadora", "cnpj", "razao_social", "modalidade", "uf"]:
            if col not in df.columns:
                df[col] = None
        if "cd_operadora" in df.columns:
            df["cd_operadora"] = df["cd_operadora"].astype(str).str.zfill(6)

        # Sanitizacao textual e remocao de mojibake
        for col in ["razao_social", "nome_fantasia", "municipio", "modalidade"]:
            if col in df.columns:
                df[col] = df[col].apply(self._fix_text_mojibake)
                df[col] = df[col].astype(str).str.strip().str.replace(r"\s+", " ", regex=True)
                df.loc[df[col].isin(["", "None", "nan", "NULL", "none", "<NA>"]), col] = None

        if "modalidade" in df.columns:
            df["modalidade"] = df["modalidade"].fillna("NÃO INFORMADA")

        return df

    def _parse_beneficiarios(self, df: pd.DataFrame) -> pd.DataFrame:
        mapa = {
            "CD_MUNICIPIO": "cd_municipio_ibge", "CD_MUNICIPIO_IBGE": "cd_municipio_ibge", "CO_MUNICIPIO_IBGE": "cd_municipio_ibge", "CODIGO_MUNICIPIO": "cd_municipio_ibge",
            "SG_UF": "uf", "UF": "uf", "SIGLA_UF": "uf",
            "CD_OPERADORA": "cd_operadora", "REGISTRO_OPERADORA": "cd_operadora",
            "RAZAO_SOCIAL": "razao_social", "NOME_OPERADORA": "razao_social",
            "MODALIDADE": "modalidade",
            "NR_BENEFICIARIOS_ATIVOS": "nr_beneficiarios_ativos", "BENEFICIARIOS": "nr_beneficiarios_ativos",
            "BENEFICIARIOS_MEDICO_HOSPITALAR": "nr_beneficiarios_ativos", "QT_BENEFICIARIOS": "nr_beneficiarios_ativos",
            "COMPETENCIA": "competencia", "ANO_MES": "competencia", "COMPETENCIA_ANO_MES": "competencia",
        }
        df = df.rename(columns={k: v for k, v in mapa.items() if k in df.columns})
        df = df.loc[:, ~df.columns.duplicated()].copy()
        if "nr_beneficiarios_ativos" not in df.columns:
            col_ben = next((c for c in df.columns if "BENEFICIARIO" in c.upper() or "BENEF" in c.upper()), None)
            if col_ben:
                df["nr_beneficiarios_ativos"] = pd.to_numeric(df[col_ben], errors="coerce").astype("Int64")
            else:
                df["nr_beneficiarios_ativos"] = pd.Series([None] * len(df), dtype="Int64")
        else:
            df["nr_beneficiarios_ativos"] = pd.to_numeric(
                df["nr_beneficiarios_ativos"], errors="coerce"
            ).astype("Int64")

        if "cd_municipio_ibge" in df.columns:
            df["cd_municipio_ibge"] = df["cd_municipio_ibge"].astype("string").str.extract(r"^(\d{6,7})")[0]

        for col in ["razao_social", "modalidade"]:
            if col in df.columns:
                df[col] = df[col].apply(self._fix_text_mojibake)
                df[col] = df[col].astype(str).str.strip().str.replace(r"\s+", " ", regex=True)

        return df

    def _parse_ressarcimento(self, df: pd.DataFrame) -> pd.DataFrame:
        mapa = {
            "CD_OPERADORA": "cd_operadora", "REGISTRO_OPERADORA": "cd_operadora",
            "RAZAO_SOCIAL": "razao_social", "NOME_OPERADORA": "razao_social",
            "SG_UF": "uf", "UF": "uf",
            "CD_MUNICIPIO_IBGE": "cd_municipio_ibge",
            "NR_ABI": "nr_abi", "NUMERO_PROCESSO": "nr_abi",
            "DT_NOTIFICACAO": "dt_notificacao", "DATA_DO_AUTO": "dt_notificacao",
            "PROCEDIMENTO": "procedimento_descricao",
            "VL_NOTIFICADO": "vl_notificado_brl", "VALOR_COBRADO_ACUMULADO": "vl_notificado_brl",
            "VL_RECOLHIDO": "vl_recolhido_brl", "VALOR_PAGO_ACUMULADO": "vl_recolhido_brl",
            "ST_COBRANCA": "status_cobranca", "STATUS_AUTO": "status_cobranca",
        }
        df = df.rename(columns={k: v for k, v in mapa.items() if k in df.columns})
        df = df.loc[:, ~df.columns.duplicated()].copy()
        for required_col, default_val in [
            ("uf", "BR"),
            ("razao_social", "CONSOLIDADO NACIONAL ANS"),
            ("cd_operadora", "000000"),
            ("status_cobranca", "EM_ANALISE"),
            ("vl_notificado_brl", 0.0),
            ("vl_recolhido_brl", 0.0),
        ]:
            if required_col not in df.columns:
                df[required_col] = default_val

        if "status_cobranca" in df.columns:
            df["status_cobranca"] = (
                df["status_cobranca"].astype(str)
                .str.strip()
                .str.upper()
                .replace({
                    "NONE": "EM_ANALISE",
                    "NAN": "EM_ANALISE",
                    "NULL": "EM_ANALISE",
                    "": "EM_ANALISE"
                })
            )

        for col in ["vl_notificado_brl", "vl_recolhido_brl"]:
            if col in df.columns:
                df[col] = (
                    df[col].astype(str)
                    .str.replace(".", "", regex=False)
                    .str.replace(",", ".", regex=False)
                    .pipe(pd.to_numeric, errors="coerce")
                    .fillna(0.0)
                )

        # Sanitizacao de textos
        for col in ["razao_social", "procedimento_descricao"]:
            if col in df.columns:
                df[col] = df[col].apply(self._fix_text_mojibake)
                df[col] = df[col].astype(str).str.strip().str.replace(r"\s+", " ", regex=True)

        # Trava financeira: Impugnações e Recursos não possuem recolhimento efetivo
        mask_sem_recolhimento = df["status_cobranca"].astype(str).str.upper().str.contains("IMPUGNAD|RECURSO", na=False)
        df.loc[mask_sem_recolhimento, "vl_recolhido_brl"] = 0.0

        if "cd_municipio_ibge" in df.columns:
            df["cd_municipio_ibge"] = df["cd_municipio_ibge"].astype("string").str.extract(r"^(\d{6,7})")[0]

        return df

    def _parse_nip(self, df: pd.DataFrame) -> pd.DataFrame:
        mapa = {
            "CD_OPERADORA": "cd_operadora", "REGISTRO_OPERADORA": "cd_operadora",
            "RAZAO_SOCIAL": "razao_social", "NOME_OPERADORA": "razao_social",
            "SG_UF": "uf", "UF_ONDE_CONSUMIDOR": "uf", "ESTADO_DO_BENEFICIARIO": "uf",
            "DS_MOTIVO_NEGATIVA": "motivo_negativa", "ASSUNTO": "motivo_negativa",
            "DS_DESFECHO": "desfecho_nip", "SITUACAO_DA_NIP": "desfecho_nip", "CLASSIFICACAO_DA_NIP": "desfecho_nip",
            "NR_NOTIFICACOES": "nr_notificacoes",
            "COMPETENCIA": "competencia",
        }
        df = df.rename(columns={k: v for k, v in mapa.items() if k in df.columns})
        df = df.loc[:, ~df.columns.duplicated()].copy()
        if "nr_notificacoes" not in df.columns:
            df["nr_notificacoes"] = 1
        else:
            df["nr_notificacoes"] = pd.to_numeric(
                df["nr_notificacoes"], errors="coerce"
            ).fillna(1).astype(int)

        for col in ["razao_social", "motivo_negativa", "desfecho_nip"]:
            if col in df.columns:
                df[col] = df[col].apply(self._fix_text_mojibake)
                df[col] = df[col].astype(str).str.strip().str.replace(r"\s+", " ", regex=True)

        return df

    def _descompactar_zip_csv(self, content: bytes) -> pd.DataFrame:
        with zipfile.ZipFile(io.BytesIO(content)) as zf:
            csv_files = [n for n in zf.namelist() if n.lower().endswith(".csv")]
            if not csv_files:
                raise ValueError("Nenhum CSV encontrado no ZIP da ANS.")
            with zf.open(csv_files[0]) as f:
                raw_bytes = f.read()
                return pd.read_csv(io.StringIO(self._decode_content_safe(raw_bytes)), sep=";", dtype=str)

    def _ler_local(self, path: str) -> pd.DataFrame:
        if path.endswith(".csv"):
            with open(path, "rb") as f:
                return pd.read_csv(io.StringIO(self._decode_content_safe(f.read())), sep=";", dtype=str)
        if path.endswith(".zip"):
            with open(path, "rb") as f:
                return self._descompactar_zip_csv(f.read())
        return pd.read_parquet(path)
