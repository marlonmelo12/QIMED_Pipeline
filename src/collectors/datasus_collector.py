"""
DATASUS Ingestion Collector for QIMED DataQore.
Downloads real DBC files via FTP from ftp.datasus.gov.br,
decompresses DBC -> DBF using pyreaddbc, and parses records into DataFrames.
"""
import os
import ftplib
import tempfile
from typing import Optional, Any, Dict
import pandas as pd
from dbfread import DBF

try:
    import pyreaddbc
except ImportError:
    pyreaddbc = None

from src.collectors.base import BaseCollector, CollectorConfig
from src.utils.logging_config import get_logger

logger = get_logger(__name__)


class DatasusCollector(BaseCollector):
    """
    Downloads and extracts SIH and CNES microdata from DATASUS FTP servers.
    """
    FTP_HOST = "ftp.datasus.gov.br"

    def __init__(self, subsystem: str, uf: str, year: int, month: int, config: CollectorConfig = None):
        super().__init__(config)
        self.subsystem = subsystem.upper()
        self.uf = uf.upper()
        self.year = year
        self.month = month

    def get_source_type(self) -> str:
        return f"datasus_{self.subsystem.lower()}"

    def get_remote_filename(self) -> str:
        yy = str(self.year)[-2:]
        mm = f"{self.month:02d}"
        if self.subsystem == "SIH":
            return f"RD{self.uf}{yy}{mm}.dbc"
        elif self.subsystem == "CNES":
            return f"ST{self.uf}{yy}{mm}.dbc"
        else:
            raise ValueError(f"Unsupported DATASUS subsystem: {self.subsystem}")

    def fetch(self) -> str:
        """
        Connects to DATASUS FTP and downloads the requested DBC file.
        Returns local filepath of the downloaded DBC.
        """
        filename = self.get_remote_filename()

        if self.subsystem == "SIH":
            remote_dir = "/dissemin/publicos/SIHSUS/200801_/Dados/"
        elif self.subsystem == "CNES":
            remote_dir = "/dissemin/publicos/CNES/200508_/Dados/"
        else:
            raise ValueError(f"Unsupported DATASUS subsystem: {self.subsystem}")

        logger.info(f"Connecting to DATASUS FTP {self.FTP_HOST} at {remote_dir}")
        ftp = ftplib.FTP(self.FTP_HOST, timeout=30)
        ftp.login()

        ftp.cwd(remote_dir)

        temp_dir = tempfile.mkdtemp(prefix="qimed_datasus_")
        local_dbc = os.path.join(temp_dir, filename)

        logger.info(f"Downloading {filename} from DATASUS...")
        with open(local_dbc, "wb") as f:
            ftp.retrbinary(f"RETR {filename}", f.write)

        ftp.quit()
        file_size_kb = round(os.path.getsize(local_dbc) / 1024, 2)
        logger.info(f"Successfully downloaded {filename} ({file_size_kb} KB) to {local_dbc}")
        return local_dbc

    def parse(self, raw_data: Any) -> pd.DataFrame:
        """
        Decompresses DBC -> DBF and reads into a pandas DataFrame.
        """
        dbc_path = str(raw_data)
        if not os.path.exists(dbc_path):
            raise FileNotFoundError(f"DBC file not found: {dbc_path}")

        # Target DBF path
        dbf_path = dbc_path.replace(".dbc", ".dbf").replace(".DBC", ".dbf")

        logger.info(f"Decompressing DBC: {dbc_path} -> {dbf_path}")
        if pyreaddbc is not None:
            pyreaddbc.dbc2dbf(dbc_path, dbf_path)
        else:
            raise RuntimeError("pyreaddbc library is required for DBC decompression on DATASUS files.")

        logger.info(f"Reading DBF into DataFrame: {dbf_path}")
        # Try reading DBF with fallback encodings
        for encoding in ("iso-8859-1", "latin1", "cp1252", "utf-8"):
            try:
                table = DBF(dbf_path, encoding=encoding, load=True, ignore_missing_memofile=True)
                df = pd.DataFrame(iter(table))
                break
            except UnicodeDecodeError:
                continue
            except Exception as e:
                logger.warning(f"Error reading DBF with {encoding}: {e}")
        else:
            # Fallback
            table = DBF(dbf_path, load=True, ignore_missing_memofile=True)
            df = pd.DataFrame(iter(table))

        # Standardize column headers
        df.columns = [str(col).upper().strip() for col in df.columns]
        logger.info(f"Successfully parsed {len(df)} records from {os.path.basename(dbc_path)}. Columns: {len(df.columns)}")
        return df
