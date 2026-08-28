"""
Testes unitários para os modelos e resolvedores do Pipeline Nacional (27 UFs).
"""
import pytest
import pandas as pd
from src.silver.ibge_nacional import resolver_uf_brasil, resolver_municipio_nacional
from src.silver.cid10_nacional import resolver_cid10_nacional
from src.gold.models.kpi_nacional_leitos import build_dm_nacional_ocupacao_leitos
from src.gold.models.kpi_nacional_glosas import build_dm_nacional_ranking_glosas
from src.gold.models.kpi_nacional_eficiencia import build_dm_nacional_eficiencia_mortalidade


class TestNacionalResolvers:
    """Testes dos resolvedores de terminologia geográfica e clínica."""

    def test_resolver_uf_brasil(self):
        sigla, nome, regiao = resolver_uf_brasil("35")
        assert sigla == "SP"
        assert nome == "São Paulo"
        assert regiao == "Sudeste"

        sigla, nome, regiao = resolver_uf_brasil("BA")
        assert sigla == "BA"
        assert nome == "Bahia"
        assert regiao == "Nordeste"

    def test_resolver_municipio_nacional(self):
        nome, uf, regiao = resolver_municipio_nacional("355030")
        assert nome == "São Paulo"
        assert uf == "SP"
        assert regiao == "Sudeste"

    def test_resolver_cid10_nacional(self):
        desc, cap = resolver_cid10_nacional("I21.9")
        assert "Infarto" in desc
        assert "Cardiovasculares" in cap

        desc, cap = resolver_cid10_nacional("Z30.2")
        assert "Planejamento Familiar" in desc or "Laqueadura" in desc


class TestNacionalDataMarts:
    """Testes de cálculo dos Data Marts Nacionais."""

    def test_ocupacao_leitos_calculation(self):
        df_enc = pd.DataFrame([
            {"uf": "SP", "length_of_stay_days": 1000},
            {"uf": "SP", "length_of_stay_days": 2000},
            {"uf": "RJ", "length_of_stay_days": 1500},
        ])
        dm = build_dm_nacional_ocupacao_leitos(df_enc, dias_no_mes=31)
        assert len(dm) == 2
        sp_row = dm[dm["uf_sigla"] == "SP"].iloc[0]
        assert sp_row["total_internacoes"] == 2
        assert sp_row["total_dias_permanencia"] == 3000
        assert sp_row["capacidade_dias_leito_mes"] > 0
        assert sp_row["taxa_ocupacao_leitos_pct"] > 0

    def test_ranking_glosas_calculation(self):
        df_sia = pd.DataFrame([
            {"PA_UFMUN": "350000", "PA_QTDPRO": 10, "PA_QTDAPR": 8, "PA_VALPRO": 1000.0, "PA_VALAPR": 800.0},
            {"PA_UFMUN": "330000", "PA_QTDPRO": 5, "PA_QTDAPR": 5, "PA_VALPRO": 500.0, "PA_VALAPR": 500.0}
        ])
        dm = build_dm_nacional_ranking_glosas(df_sia)
        assert len(dm) == 2
        sp_row = dm[dm["uf_sigla"] == "SP"].iloc[0]
        assert sp_row["total_glosado_brl"] == 200.0
        assert sp_row["taxa_glosa_pct"] == 20.0
