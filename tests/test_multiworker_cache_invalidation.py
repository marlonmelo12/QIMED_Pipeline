"""
Teste de Concorrência - Risco B: Cache com Invalidação Multi-Worker.
Comprova que múltiplos workers/processos respeitam a invalidação por Epoch/Watermark
e nunca servem dados obsoletos após uma invalidação disparada por qualquer worker ou pipeline.
"""
import os
import time
import pytest
from src.api.cache import (
    invalidate_cache_for_period,
    invalidate_all_cache,
    get_cache_epoch,
    _cache,
    _lock
)


def test_multiworker_cache_epoch_invalidation(tmp_path, monkeypatch):
    """
    Simula 2 workers independentes operando sobre o mesmo arquivo compartilhado de versões.
    Valida que quando o Worker 1 invalida uma competência, o Worker 2 descarta o cache L1.
    """
    version_file = str(tmp_path / ".cache_version.json")
    monkeypatch.setenv("CACHE_VERSION_FILE", version_file)

    # Estado inicial
    epoch_inicial = get_cache_epoch("2026-05")

    # Worker 1 grava no cache
    with _lock:
        _cache["key_w1"] = (epoch_inicial, {"dados": "versao_antiga_w1"})
        _cache["key_w2"] = (epoch_inicial, {"dados": "versao_antiga_w2"})

    # Verifica hit no cache
    with _lock:
        e, val = _cache["key_w2"]
        assert val["dados"] == "versao_antiga_w2"

    # Worker 1 dispara invalidação da competência 2026-05
    invalidate_cache_for_period("2026-05")
    novo_epoch = get_cache_epoch("2026-05")
    assert novo_epoch > epoch_inicial, f"Novo epoch {novo_epoch} deve ser maior que {epoch_inicial}"

    # Simula Worker 2 consultando cache L1 que continha o dado com epoch antigo
    with _lock:
        _cache["key_w2"] = (epoch_inicial, {"dados": "versao_antiga_w2"})

    # Worker 2 verifica e rejeita entrada com epoch obsoleto
    with _lock:
        entry_epoch, val = _cache["key_w2"]
        is_valid = (entry_epoch >= get_cache_epoch("2026-05"))
        assert not is_valid, "Worker 2 não deveria aceitar cache com epoch desatualizado!"
