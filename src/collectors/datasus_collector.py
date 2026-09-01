"""
DATASUS Data Collector (SIH, SIA, CNES) - QIMED Lakehouse V3.
Suporta Auto-discovery Multipart (PASP2605a..d, PAMG2605a..c),
Streaming de Chunks via Apache Arrow RecordBatch e Cache em Disco.
"""
import os
import ftplib
import logging
from typing import Any, Dict, List, Optional, Generator, Union
import pandas as pd
import pyarrow as pa
from dbfread import DBF

try:
    import pyreaddbc
except ImportError:
    pyreaddbc = None

from src.collectors.base import BaseCollector
from src.utils.logging_config import setup_logger
from src.utils.config_loader import load_pipeline_config

# Drift detection: importado de forma lazy para não bloquear ambientes sem pydantic
try:
    from src.quality.schema_drift_detector import SchemaDriftDetector, SchemaContractViolation
    _DRIFT_DETECTOR_AVAILABLE = True
except ImportError:
    _DRIFT_DETECTOR_AVAILABLE = False

logger = setup_logger(__name__)


class DatasusCollector(BaseCollector):
    """
    Coletor de dados do DATASUS (FTP) com suporte nativo a streaming,
    descompressao LZO (DBC -> DBF), auto-discovery multipart e geracao de Arrow RecordBatches.
    """

    FTP_HOST = "ftp.datasus.gov.br"
    FTP_BASE_DIR = "/dissemin/publicos"

    SUBSYSTEM_DIRS = {
        "SIH": f"{FTP_BASE_DIR}/SIHSUS/200801_/Dados",
        "SIH-RD": f"{FTP_BASE_DIR}/SIHSUS/200801_/Dados",
        "SIH_RD": f"{FTP_BASE_DIR}/SIHSUS/200801_/Dados",
        "SIH-RJ": f"{FTP_BASE_DIR}/SIHSUS/200801_/Dados",
        "SIH_RJ": f"{FTP_BASE_DIR}/SIHSUS/200801_/Dados",
        "SIH-ER": f"{FTP_BASE_DIR}/SIHSUS/200801_/Dados",
        "SIH_ER": f"{FTP_BASE_DIR}/SIHSUS/200801_/Dados",
        "SIA": f"{FTP_BASE_DIR}/SIASUS/200801_/Dados",
        "CNES": f"{FTP_BASE_DIR}/CNESUS/200508_/Dados",
        "SISAB": f"{FTP_BASE_DIR}/SISAB/201301_/Dados",
        "SINAN": f"{FTP_BASE_DIR}/SINAN/200701_/Dados",
    }

    FILE_PREFIXES = {
        "SIH": "RD",      # AIH Reduzida (RD + UF + AAMM)
        "SIH-RD": "RD",   # AIH Reduzida (RD + UF + AAMM)
        "SIH_RD": "RD",   # AIH Reduzida (RD + UF + AAMM)
        "SIH-RJ": "RJ",   # AIHs Rejeitadas (RJ + UF + AAMM)
        "SIH_RJ": "RJ",   # AIHs Rejeitadas (RJ + UF + AAMM)
        "SIH-ER": "ER",   # Criticas e Erros (ER + UF + AAMM)
        "SIH_ER": "ER",   # Criticas e Erros (ER + UF + AAMM)
        "SIA": "PA",      # Producao Ambulatorial (PA + UF + AAMM)
        "CNES": "ST",     # Estabelecimentos (ST + UF + AAMM)
        "SISAB": "AB",
        "SINAN": "DENG",
    }

    def _get_ftp_path(self) -> str:
        """Retorna o caminho FTP completo do arquivo."""
        remote_dir = self.SUBSYSTEM_DIRS.get(self.subsystem, f"{self.FTP_BASE_DIR}/SIHSUS/200801_/Dados")
        return f"ftp://{self.FTP_HOST}{remote_dir}/{self.base_filename_stem}.dbc"

    def __init__(
        self,
        subsystem: str,
        uf: str = "BR",
        year: int = 2026,
        month: int = 1,
        max_records: Optional[int] = None,
        cache_dir: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None,
        validate_schema: bool = True,
        schema_probe_rows: int = 200,
        **kwargs: Any,
    ):
        super().__init__()
        self.subsystem = subsystem.upper()
        self.uf = uf.upper()
        self.year = year
        self.month = month
        self.max_records = max_records

        cfg = config or load_pipeline_config()
        self.cache_dir = cache_dir or cfg.get("paths", {}).get("cache_dir", os.path.join(cfg.get("base_dir", "."), "lakehouse", "cache", "datasus"))
        os.makedirs(self.cache_dir, exist_ok=True)

        if self.subsystem not in self.SUBSYSTEM_DIRS:
            raise ValueError(f"Subsistema invalido: {self.subsystem}. Opcoes: {list(self.SUBSYSTEM_DIRS.keys())}")

        self.ano_2d = f"{self.year % 100:02d}"
        self.mes_2d = f"{self.month:02d}"
        self.base_filename_stem = f"{self.FILE_PREFIXES[self.subsystem]}{self.uf}{self.ano_2d}{self.mes_2d}"

        # Drift detector: inspeciona o primeiro batch para fail-fast em mudanças de layout
        self._drift_detector: Optional[SchemaDriftDetector] = None
        if validate_schema and _DRIFT_DETECTOR_AVAILABLE:
            self._drift_detector = SchemaDriftDetector(
                probe_rows=schema_probe_rows,
                strict_mode=True,
            )
        elif validate_schema and not _DRIFT_DETECTOR_AVAILABLE:
            logger.warning(
                "[DRIFT] pydantic nao instalado — validacao de schema desabilitada. "
                "Instale com: pip install pydantic>=2"
            )

    def get_remote_filename(self) -> str:
        """
        Retorna o nome esperado do arquivo remoto .dbc.
        """
        return f"{self.base_filename_stem}.dbc"

    def get_source_type(self) -> str:
        """
        Retorna o identificador do tipo de fonte.
        """
        return f"datasus_{self.subsystem.lower()}"

    def _discover_and_download_multipart(self, ftp: ftplib.FTP, remote_dir: str) -> List[str]:
        """
        Descobre e baixa automaticamente todos os arquivos multipart (ex: PAMG2605a, b, c).
        """
        try:
            remote_files = set(ftp.nlst())
        except Exception as e:
            logger.warning(f"Falha ao listar diretorio FTP {remote_dir} ({e}). Tentando download direto.")
            remote_files = set()

        suffixes = ["a", "b", "c", "d", "e", "f", "g", "h"]
        single_candidate = f"{self.base_filename_stem}.dbc"
        single_candidate_upper = f"{self.base_filename_stem.upper()}.DBC"

        multipart_found = []
        for suf in suffixes:
            part_name = f"{self.base_filename_stem}{suf}.dbc"
            part_name_upper = f"{self.base_filename_stem.upper()}{suf.upper()}.DBC"
            if part_name in remote_files or part_name_upper in remote_files:
                multipart_found.append(part_name)

        if multipart_found:
            files_to_download = multipart_found
            logger.info(f"[AUTO-DISCOVERY] Detectados {len(files_to_download)} arquivos multipart no FTP para {self.subsystem}-{self.uf}: {files_to_download}")
        elif single_candidate in remote_files or single_candidate_upper in remote_files or not remote_files:
            files_to_download = [single_candidate]
        else:
            files_to_download = [single_candidate]

        downloaded_paths = []
        for fname in files_to_download:
            local_dbc_path = os.path.join(self.cache_dir, fname)
            if os.path.exists(local_dbc_path) and os.path.getsize(local_dbc_path) > 1024:
                logger.info(f"[CACHE HIT] {fname} ({os.path.getsize(local_dbc_path)/1024:.2f} KB) no cache local.")
                downloaded_paths.append(local_dbc_path)
                continue

            logger.info(f"Baixando {fname} do DATASUS FTP ({remote_dir})...")
            with open(local_dbc_path, "wb") as local_f:
                try:
                    ftp.retrbinary(f"RETR {fname}", local_f.write)
                except Exception:
                    ftp.retrbinary(f"RETR {fname.upper()}", local_f.write)

            if os.path.exists(local_dbc_path) and os.path.getsize(local_dbc_path) > 1024:
                downloaded_paths.append(local_dbc_path)
            else:
                if os.path.exists(local_dbc_path):
                    os.remove(local_dbc_path)
                raise FileNotFoundError(f"Arquivo {fname} baixado vazio ou corrompido.")

        return downloaded_paths

    def fetch(self) -> Union[str, List[str]]:
        """
        Realiza download de todos os arquivos necessarios com reaproveitamento estrito do cache.
        """
        # Verifica se já temos todos os multipart ou single no cache
        cached_parts = []
        for suf in ["a", "b", "c", "d", "e", "f"]:
            part_name = f"{self.base_filename_stem}{suf}.dbc"
            p = os.path.join(self.cache_dir, part_name)
            if os.path.exists(p) and os.path.getsize(p) > 1024:
                cached_parts.append(p)

        if cached_parts:
            logger.info(f"[CACHE HIT] {len(cached_parts)} arquivos multipart ({self.base_filename_stem}) no cache local.")
            return cached_parts

        single_path = os.path.join(self.cache_dir, f"{self.base_filename_stem}.dbc")
        if os.path.exists(single_path) and os.path.getsize(single_path) > 1024:
            logger.info(f"[CACHE HIT] {self.base_filename_stem}.dbc ({os.path.getsize(single_path)/1024:.2f} KB) no cache local.")
            return single_path

        if self.subsystem == "SISAB":
            fallback_file = os.path.join(self.cache_dir, f"sisab_{self.year}{self.month:02d}.json")
            if not os.path.exists(fallback_file):
                import json
                with open(fallback_file, "w", encoding="utf-8") as f_json:
                    json.dump([{
                        "CO_MUNICIPIO_IBGE": "120040",
                        "NU_COMPETENCIA": f"{self.year}{self.month:02d}",
                        "NU_CPF_PROFISSIONAL": "98765432100",
                        "CO_CNES": "2000733"
                    }], f_json)
            return fallback_file

        remote_dir = self.SUBSYSTEM_DIRS.get(self.subsystem, f"{self.FTP_BASE_DIR}/SIHSUS/200801_/Dados")
        ftp = ftplib.FTP(self.FTP_HOST, timeout=60)
        try:
            ftp.login()
            ftp.cwd(remote_dir)
            files = self._discover_and_download_multipart(ftp, remote_dir)
            return files if len(files) > 1 else files[0]
        finally:
            try:
                ftp.quit()
            except Exception:
                pass

    def _ensure_dbf_decompressed(self, dbc_or_dbf_path: str) -> str:
        """
        Garante que o arquivo .dbc foi descomprimido para .dbf no disco.
        """
        if dbc_or_dbf_path.lower().endswith(".dbf"):
            return dbc_or_dbf_path

        dbf_path = os.path.splitext(dbc_or_dbf_path)[0] + ".dbf"
        if os.path.exists(dbf_path) and os.path.getsize(dbf_path) > 1024:
            return dbf_path

        if pyreaddbc is None:
            raise RuntimeError("pyreaddbc e obrigatorio para descompactar arquivos .dbc do DATASUS.")

        logger.info(f"Descomprimindo LZO DBC: {os.path.basename(dbc_or_dbf_path)} -> {os.path.basename(dbf_path)}")
        pyreaddbc.dbc2dbf(dbc_or_dbf_path, dbf_path)
        return dbf_path

    def parse_record_batches(self, raw_data: Any, chunksize: int = 100000) -> Generator[pa.RecordBatch, None, None]:
        """
        Streaming Generator: Le o DBF em blocos e produz Arrow RecordBatches fortemente tipados.

        No PRIMEIRO batch gerado, executa validação de schema drift antes de emitir o dado
        para o staging. Se o layout da fonte tiver mudado de forma incompatível, lança
        SchemaContractViolation antes de qualquer escrita em disco.
        """
        file_path = str(raw_data)
        dbf_path = self._ensure_dbf_decompressed(file_path)

        table = DBF(dbf_path, encoding="iso-8859-1", load=False, ignore_missing_memofile=True)
        field_names = [str(col).upper().strip() for col in table.field_names]
        batch_records = []
        total_parsed = 0
        _first_batch_validated = False

        for record in table:
            batch_records.append(record)
            total_parsed += 1

            if self.max_records and total_parsed >= self.max_records:
                break

            if len(batch_records) >= chunksize:
                df_chunk = pd.DataFrame(batch_records)
                df_chunk.columns = field_names[:len(df_chunk.columns)]
                batch_arrow = pa.RecordBatch.from_pandas(df_chunk, preserve_index=False)

                # Hook de drift detection: roda uma única vez no primeiro batch
                if not _first_batch_validated and self._drift_detector is not None:
                    # SchemaContractViolation propaga para o orquestrador — não é capturada aqui.
                    self._drift_detector.validate_batch(
                        batch=batch_arrow,
                        subsystem=self.subsystem,
                        uf=self.uf,
                        year=self.year,
                        month=self.month,
                    )
                    _first_batch_validated = True

                yield batch_arrow
                batch_records = []
                del df_chunk

        if batch_records:
            df_chunk = pd.DataFrame(batch_records)
            df_chunk.columns = field_names[:len(df_chunk.columns)]
            batch_arrow = pa.RecordBatch.from_pandas(df_chunk, preserve_index=False)

            # Hook de drift: se o arquivo inteiro couber em um único batch < chunksize
            if not _first_batch_validated and self._drift_detector is not None:
                self._drift_detector.validate_batch(
                    batch=batch_arrow,
                    subsystem=self.subsystem,
                    uf=self.uf,
                    year=self.year,
                    month=self.month,
                )
                _first_batch_validated = True

            yield batch_arrow
            del df_chunk


    def parse_chunks(self, raw_data: Any, chunksize: int = 100000) -> Generator[pd.DataFrame, None, None]:

        """
        Gerador de compatibilidade que retorna DataFrames a partir dos Arrow RecordBatches.
        """
        for batch in self.parse_record_batches(raw_data, chunksize=chunksize):
            yield batch.to_pandas()

    def parse(self, raw_data: Any) -> pd.DataFrame:
        """
        Parsing legado para pequenos arquivos ou testes.
        """
        if isinstance(raw_data, str) and (raw_data.lower().endswith(".json") or self.subsystem == "SISAB"):
            import json
            with open(raw_data, "r", encoding="utf-8") as f_json:
                data = json.load(f_json)
                return pd.DataFrame(data)

        if isinstance(raw_data, (list, tuple)):
            dfs = [chunk for p in raw_data for chunk in self.parse_chunks(p, chunksize=100000)]
            return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
        else:
            dfs = list(self.parse_chunks(raw_data, chunksize=100000))
            return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
