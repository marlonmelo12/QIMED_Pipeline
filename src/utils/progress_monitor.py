#!/usr/bin/env python3
import os
import sys
import time
import json

PROGRESS_FILE = "/opt/qimed/lakehouse/pipeline_progress.json"

def format_bar(pct, width=30):
    if pct is None:
        pct = 0.0
    filled = int(width * (pct / 100.0))
    bar = "=" * filled + "-" * (width - filled)
    return f"[{bar}] {pct:.1f}%"

def main():
    print("=" * 70)
    print(" MONITOR DE EXECUCAO DO PIPELINE QIMED LAKEHOUSE")
    print("=" * 70)
    
    if not os.path.exists(PROGRESS_FILE):
        print("Aguardando inicio do pipeline...")
        return

    with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    timestamp = data.get("timestamp", "N/A")
    etapa = data.get("etapa", "N/A")
    detalhe = data.get("detalhe", "N/A")
    pct = data.get("progresso_pct", 0.0)
    extra = data.get("extra", {})

    print(f"Status em: {timestamp}")
    print(f"Etapa:     {etapa}")
    print(f"Detalhe:   {detalhe}")
    print(f"Progresso: {format_bar(pct)}")
    if extra:
        print(f"Metadados: {json.dumps(extra, ensure_ascii=False)}")
    print("=" * 70)

if __name__ == "__main__":
    main()
