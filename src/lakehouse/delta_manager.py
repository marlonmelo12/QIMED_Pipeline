"""
Gerenciador do Delta Lake para o QIMED Lakehouse.
Responsável por listagem, time-travel, controle de versões e otimização/compactação de arquivos Delta.
"""
import os
from typing import List, Dict, Any, Optional
from deltalake import DeltaTable

from src.utils.logging_config import setup_logger

logger = setup_logger(__name__)


class DeltaManager:
    """
    Gerencia tabelas Delta no Lakehouse (criação, listagem, time-travel e otimização).
    """

    def __init__(self, lakehouse_path: str = None):
        """
        Inicializa o DeltaManager com o caminho raiz do Lakehouse.
        """
        self.lakehouse_path = lakehouse_path or os.getenv("LAKEHOUSE_PATH", "/tmp/lakehouse/bronze")

    def get_table(self, source: str, subsystem: str) -> Optional[DeltaTable]:
        """
        Recupera o objeto DeltaTable para a fonte e subsistema informados.
        """
        table_path = os.path.join(self.lakehouse_path, source, subsystem)
        try:
            if os.path.exists(table_path):
                return DeltaTable(table_path)
            logger.warning(f"Caminho da tabela não existe: {table_path}")
            return None
        except Exception as e:
            logger.error(f"Erro ao carregar DeltaTable em {table_path}: {e}")
            return None

    def list_partitions(self, source: str, subsystem: str) -> List[str]:
        """
        Lista as colunas de partição de uma tabela Delta.
        """
        table = self.get_table(source, subsystem)
        if not table:
            return []
        
        try:
            return table.metadata().partition_columns
        except Exception as e:
            logger.error(f"Erro ao listar partições de {source}/{subsystem}: {e}")
            return []

    def get_version_history(self, source: str, subsystem: str) -> List[Dict[str, Any]]:
        """
        Obtém o histórico de versões e auditoria (time-travel) de uma tabela Delta.
        """
        table = self.get_table(source, subsystem)
        if not table:
            return []
        
        try:
            return table.history()
        except Exception as e:
            logger.error(f"Erro ao recuperar histórico de {source}/{subsystem}: {e}")
            return []

    def query_as_of_version(self, source: str, subsystem: str, version: int):
        """
        Consulta uma versão histórica específica da tabela Delta (time-travel).
        """
        table_path = os.path.join(self.lakehouse_path, source, subsystem)
        try:
            dt = DeltaTable(table_path, version=version)
            return dt.to_pyarrow_dataset()
        except Exception as e:
            logger.error(f"Erro ao consultar versão {version} para {source}/{subsystem}: {e}")
            raise

    def compact_files(self, source: str, subsystem: str) -> Dict[str, Any]:
        """
        Compacta pequenos arquivos na tabela Delta (Optimize / Compaction).
        """
        table = self.get_table(source, subsystem)
        if not table:
            return {"status": "skipped", "reason": "Tabela não encontrada"}
        
        try:
            logger.info(f"Compactando arquivos para {source}/{subsystem}...")
            metrics = table.optimize.compact()
            logger.info(f"Compactação concluída com sucesso. Métricas: {metrics}")
            return {"status": "success", "metrics": metrics}
        except Exception as e:
            logger.error(f"Erro ao compactar arquivos para {source}/{subsystem}: {e}")
            return {"status": "error", "error": str(e)}
