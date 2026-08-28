"""
Testes unitarios para o modulo ANS: Saude Suplementar e Ressarcimento ao SUS.
"""
import pytest
import pandas as pd
import sys
from src.collectors.ans_collector import AnsCollector


@pytest.fixture(autouse=True)
def mock_ans_fetch(monkeypatch):
    """Evita chamadas de rede externas para o servidor da ANS durante testes unitários."""
    def mock_fetch(self):
        if self.modalidade == "operadoras":
            return pd.DataFrame([
                {"Registro_ANS": "000001", "CNPJ": "11111111000111", "Razao_Social": "BRADESCO SAÚDE S.A.", "Modalidade": "Seguradora Especializada em Saúde", "UF": "SP", "Situacao": "ATIVA"}
            ])
        elif self.modalidade == "beneficiarios":
            return pd.DataFrame([
                {"CD_OPERADORA": "000001", "NR_BENEFICIARIOS_ATIVOS": "15000", "UF": "CE", "COMPETENCIA": "202605"}
            ])
        elif self.modalidade == "ressarcimento":
            return pd.DataFrame([
                {"CD_OPERADORA": "000001", "RAZAO_SOCIAL": "BRADESCO SAÚDE S.A.", "UF": "CE", "NR_ABI": "ABI-001", "VL_NOTIFICADO": "1000,00", "VL_RECOLHIDO": "500,00", "ST_COBRANCA": "RECOLHIDO"}
            ])
        elif self.modalidade == "nip":
            return pd.DataFrame([
                {"CD_OPERADORA": "000001", "RAZAO_SOCIAL": "BRADESCO SAÚDE S.A.", "UF": "CE", "DS_MOTIVO_NEGATIVA": "NEGATIVA_DE_COBERTURA", "DS_DESFECHO": "RESOLVIDO", "NR_NOTIFICACOES": "2"}
            ])
        return pd.DataFrame()
    monkeypatch.setattr(AnsCollector, "fetch", mock_fetch)


class TestAnsCollector:
    def test_modalidade_invalida_levanta_erro(self):
        with pytest.raises(ValueError):
            AnsCollector(modalidade="invalida")

    def test_operadoras_retorna_dataframe(self):
        col = AnsCollector(modalidade="operadoras", uf="CE", year=2026, month=5)
        df = col.parse(col.fetch())
        assert isinstance(df, pd.DataFrame) and len(df) > 0

    def test_operadoras_colunas_obrigatorias(self):
        col = AnsCollector(modalidade="operadoras", uf="CE", year=2026, month=5)
        df = col.parse(col.fetch())
        for campo in ("cd_operadora", "razao_social", "modalidade", "uf"):
            assert campo in df.columns

    def test_beneficiarios_retorna_dataframe(self):
        col = AnsCollector(modalidade="beneficiarios", uf="CE", year=2026, month=5)
        df = col.parse(col.fetch())
        assert isinstance(df, pd.DataFrame) and len(df) > 0

    def test_beneficiarios_nr_ativos_nao_negativo(self):
        col = AnsCollector(modalidade="beneficiarios", uf="CE", year=2026, month=5)
        df = col.parse(col.fetch())
        assert (df["nr_beneficiarios_ativos"] >= 0).all()

    def test_ressarcimento_retorna_dataframe(self):
        col = AnsCollector(modalidade="ressarcimento", uf="CE", year=2026, month=5)
        df = col.parse(col.fetch())
        assert isinstance(df, pd.DataFrame) and len(df) > 0

    def test_ressarcimento_valores_nao_negativos(self):
        col = AnsCollector(modalidade="ressarcimento", uf="CE", year=2026, month=5)
        df = col.parse(col.fetch())
        assert (df["vl_notificado_brl"] >= 0).all()
        assert (df["vl_recolhido_brl"] >= 0).all()

    def test_nip_retorna_dataframe(self):
        col = AnsCollector(modalidade="nip", uf="CE", year=2026, month=5)
        df = col.parse(col.fetch())
        assert isinstance(df, pd.DataFrame) and len(df) > 0

    def test_nip_campos_obrigatorios(self):
        col = AnsCollector(modalidade="nip", uf="CE", year=2026, month=5)
        df = col.parse(col.fetch())
        for campo in ("motivo_negativa", "desfecho_nip", "nr_notificacoes"):
            assert campo in df.columns

    def test_nip_nr_notificacoes_nao_negativo(self):
        col = AnsCollector(modalidade="nip", uf="CE", year=2026, month=5)
        df = col.parse(col.fetch())
        assert (df["nr_notificacoes"] >= 0).all()

    def test_status_cobranca_valido(self):
        col = AnsCollector(modalidade="ressarcimento", uf="CE", year=2026, month=5)
        df = col.parse(col.fetch())
        if "status_cobranca" in df.columns:
            validos = {"RECOLHIDO", "INADIMPLENTE", "EM_RECURSO", "EM_ANALISE", "PENDENTE", "IMPUGNADO", "PAGO", "RECOLHIDO AO FNS"}
            assert set(df["status_cobranca"].unique()) <= validos

    def test_fallback_status_em_analise(self):
        """Valida que quando status_cobranca é omitido/nulo, o default prudente é EM_ANALISE."""
        col = AnsCollector(modalidade="ressarcimento")
        raw_df = pd.DataFrame([
            {"CD_OPERADORA": "000001", "VL_NOTIFICADO": "1000,00", "VL_RECOLHIDO": "0,00"},
            {"CD_OPERADORA": "000002", "ST_COBRANCA": None, "VL_NOTIFICADO": "500,00", "VL_RECOLHIDO": "0,00"},
            {"CD_OPERADORA": "000003", "ST_COBRANCA": "", "VL_NOTIFICADO": "300,00", "VL_RECOLHIDO": "0,00"},
        ])
        parsed = col._parse_ressarcimento(raw_df)
        assert (parsed["status_cobranca"] == "EM_ANALISE").all(), f"Esperado 'EM_ANALISE' como fallback, obteve: {parsed['status_cobranca'].tolist()}"
        assert "RECOLHIDO" not in parsed["status_cobranca"].values

    def test_coerencia_financeira_impugnacao(self):
        """Valida que cobranças impugnadas possuem valor recolhido zerado (0.0)."""
        col = AnsCollector(modalidade="ressarcimento")
        raw_df = pd.DataFrame([
            {"CD_OPERADORA": "000001", "ST_COBRANCA": "IMPUGNADO", "VL_NOTIFICADO": "5000,00", "VL_RECOLHIDO": "5000,00"},
            {"CD_OPERADORA": "000002", "ST_COBRANCA": "IMPUGNADA", "VL_NOTIFICADO": "2000,00", "VL_RECOLHIDO": "1000,00"},
            {"CD_OPERADORA": "000003", "ST_COBRANCA": "RECOLHIDO", "VL_NOTIFICADO": "3000,00", "VL_RECOLHIDO": "3000,00"},
        ])
        parsed = col._parse_ressarcimento(raw_df)
        impugnados = parsed[parsed["status_cobranca"].str.contains("IMPUGNAD")]
        assert (impugnados["vl_recolhido_brl"] == 0.0).all(), "Cobranças sob impugnação devem ter vl_recolhido_brl = 0.0"
        recolhido = parsed[parsed["status_cobranca"] == "RECOLHIDO"]
        assert (recolhido["vl_recolhido_brl"] == 3000.0).all()

    def test_source_type_inclui_modalidade(self):
        for mod in ("operadoras", "beneficiarios", "ressarcimento", "nip"):
            col = AnsCollector(modalidade=mod)
            assert mod in col.get_source_type()

    def test_uf_normalizada_maiusculas(self):
        col = AnsCollector(modalidade="operadoras", uf="ce")
        assert col.uf == "CE"

    def test_taxa_recuperacao_max_100(self):
        col = AnsCollector(modalidade="ressarcimento", uf="CE", year=2026, month=5)
        df = col.parse(col.fetch())
        df["taxa"] = df["vl_recolhido_brl"] / df["vl_notificado_brl"].replace(0, float("inf")) * 100
        assert (df["taxa"].fillna(0) <= 100.001).all()

    def test_ausencia_mojibake_e_sanitizacao_fct_ressarcimento_sus(self):
        """Valida ausência de mojibake e coerência financeira na tabela física fct_ressarcimento_sus."""
        import os
        import duckdb

        db_path = "warehouse/qimed_silver_completa.duckdb"
        assert os.path.exists(db_path), f"Arquivo {db_path} nao encontrado."

        conn = duckdb.connect(db_path, read_only=True)
        
        # 1. Impugnações com valor recolhido zerado
        cnt_impugnados_invalidos = conn.execute("""
            SELECT COUNT(*) 
            FROM fct_ressarcimento_sus 
            WHERE situacao_cobranca LIKE '%IMPUGNAD%' AND valor_recolhido_brl > 0
        """).fetchone()[0]
        
        # 2. Ausência de sequências de mojibake
        cnt_mojibake = conn.execute("""
            SELECT COUNT(*) 
            FROM fct_ressarcimento_sus 
            WHERE razao_social_operadora LIKE '%Ã\x8d%' 
               OR razao_social_operadora LIKE '%Ã\x93%' 
               OR razao_social_operadora LIKE '%Ã\x9a%' 
               OR razao_social_operadora LIKE '%Ã\x87%' 
               OR razao_social_operadora LIKE '%Ã\x83%' 
               OR razao_social_operadora LIKE '%ÃŠ%' 
               OR razao_social_operadora LIKE '%Ãš%' 
               OR razao_social_operadora LIKE '%Ã‰%' 
               OR razao_social_operadora LIKE '%Ã“%' 
               OR razao_social_operadora LIKE '%Ã‡%' 
               OR razao_social_operadora LIKE '%Ãƒ%' 
               OR razao_social_operadora LIKE '%Ãº%' 
               OR razao_social_operadora LIKE '%Ã§%' 
               OR razao_social_operadora LIKE '%Ã£%' 
               OR razao_social_operadora LIKE '%Ã©%' 
               OR razao_social_operadora LIKE '%Ã¡%' 
               OR razao_social_operadora LIKE '%Ãª%' 
               OR razao_social_operadora LIKE '%Ã­%' 
               OR razao_social_operadora LIKE '%Ã³%' 
               OR razao_social_operadora LIKE '%Ãµ%' 
               OR razao_social_operadora LIKE '%Ã´%' 
               OR razao_social_operadora LIKE '%Ã‚%' 
               OR razao_social_operadora LIKE '%Â %'
               OR razao_social_operadora LIKE '%Â°%'
               OR razao_social_operadora LIKE '%Â§%'
               OR razao_social_operadora LIKE '%â€%'
               OR razao_social_operadora LIKE '%SAÃDE%'
        """).fetchone()[0]

        conn.close()

        assert cnt_impugnados_invalidos == 0, f"Encontrados {cnt_impugnados_invalidos} registros impugnados com valor recolhido > 0."
        assert cnt_mojibake == 0, f"Encontrados {cnt_mojibake} registros de ressarcimento com mojibake."

    def test_resolucao_relacional_razao_social_fct_ressarcimento_sus(self):
        """Valida que fct_ressarcimento_sus possui razões sociais consistentes com dim_operadoras_saude."""
        import os
        import duckdb

        db_path = "warehouse/qimed_silver_completa.duckdb"
        assert os.path.exists(db_path), f"Arquivo {db_path} nao encontrado."

        conn = duckdb.connect(db_path, read_only=True)
        divergencias = conn.execute("""
            SELECT COUNT(*) 
            FROM fct_ressarcimento_sus f
            JOIN dim_operadoras_saude o ON f.codigo_registro_ans = o.codigo_registro_ans
            WHERE f.razao_social_operadora <> o.razao_social
        """).fetchone()[0]
        conn.close()

        assert divergencias == 0, f"Encontradas {divergencias} divergencias de razao social entre fct_ressarcimento_sus e dim_operadoras_saude."

