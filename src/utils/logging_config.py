"""
Configuração de Logs Estruturados em JSON para o QIMED DataQore.
Padroniza registros de log para observabilidade e monitoramento em produção.
"""
import logging
import json
import os
import sys
from datetime import datetime
from typing import Any, Dict


class JSONFormatter(logging.Formatter):
    """
    Formatador que emite strings JSON para logs estruturados.
    """

    def format(self, record: logging.LogRecord) -> str:
        """
        Formata o registro de log como uma string JSON estruturada.
        """
        log_entry: Dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "funcName": record.funcName,
            "lineNo": record.lineno,
        }

        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_entry)


def setup_logger(name: str) -> logging.Logger:
    """
    Configura e retorna um logger com formatação JSON.
    Lê LOG_LEVEL das variáveis de ambiente (padrão: INFO).
    """
    log_level_str = os.getenv("LOG_LEVEL", "INFO").upper()
    log_level = getattr(logging, log_level_str, logging.INFO)

    logger = logging.getLogger(name)
    logger.setLevel(log_level)

    # Evita duplicação de manipuladores se o logger já estiver configurado
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JSONFormatter())
        logger.addHandler(handler)

    return logger


# Alias para compatibilidade entre módulos
get_logger = setup_logger
