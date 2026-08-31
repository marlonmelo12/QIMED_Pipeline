"""
Carregador de configuração centralizada YAML para o QIMED Lakehouse V3.
"""
import os
from typing import Any, Dict
import yaml

_CONFIG_CACHE: Dict[str, Any] = None


def load_pipeline_config(config_path: str = None) -> Dict[str, Any]:
    """
    Carrega e resolve os caminhos absolutos do arquivo config/pipeline.yaml.
    """
    global _CONFIG_CACHE
    if _CONFIG_CACHE is not None and config_path is None:
        return _CONFIG_CACHE

    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    if not config_path:
        config_path = os.path.join(base_dir, "config", "pipeline.yaml")

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Arquivo de configuracao nao encontrado: {config_path}")

    with open(config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    # Resolve caminhos relativos para absolutos com base no base_dir
    paths = cfg.get("paths", {})
    for k, v in paths.items():
        if not os.path.isabs(v):
            paths[k] = os.path.normpath(os.path.join(base_dir, v))

    cfg["paths"] = paths
    cfg["base_dir"] = base_dir

    _CONFIG_CACHE = cfg
    return cfg


def get_config_value(section: str, key: str, default: Any = None) -> Any:
    """
    Obtém um valor espec?fico da configuração de forma segura.
    """
    cfg = load_pipeline_config()
    return cfg.get(section, {}).get(key, default)
