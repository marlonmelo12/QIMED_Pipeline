"""
Testes Automatizados dos Conectores SIH-RJ, SIH-ER e Fato Glosas Hospitalares (Task 9).
"""
import duckdb
import pytest
import pandas as pd
from src.collectors.datasus_collector import DatasusCollector
from src.gold.models.kpi_glosas_auditoria import build_dm_motivos_glosas_hospitalares


def test_datasus_collector_ftp_stem_sih_rj_er():
    """Valida mapeamento de prefixos FTP para SIH-RJ (RJ) e SIH-ER (ER) [Task 9]."""
    col_rj = DatasusCollector(subsystem="SIH-RJ", uf="CE", year=2026, month=5)
    assert col_rj.base_filename_stem == "RJCE2605"
    assert "/SIHSUS/200801_/Dados" in col_rj.SUBSYSTEM_DIRS[col_rj.subsystem]
    assert col_rj.get_source_type() == "datasus_sih-rj"

    col_er = DatasusCollector(subsystem="SIH-ER", uf="MG", year=2026, month=7)
    assert col_er.base_filename_stem == "ERMG2607"
    assert "/SIHSUS/200801_/Dados" in col_er.SUBSYSTEM_DIRS[col_er.subsystem]
    assert col_er.get_source_type() == "datasus_sih-er"


def test_fct_glosas_hospitalares_unicidade_e_preservacao():
    """Valida unicidade da PK (id_glosa_hospitalar) e preserva??o dos c?digos de erro do SIH-ER [Task 9]."""
    conn = duckdb.connect()

    df_rj = pd.DataFrame([
        {
            "N_AIH": "1234567890123",
            "CNES": "2077485",
            "MUNIC_MOV": "230440",
            "PROC_REA": "0303010037",
            "VAL_TOT": "1500.50",
            "ano": "2026",
            "mes": "05",
            "uf": "CE"
        },
        {
            "N_AIH": "1234567890124",
            "CNES": "2077485",
            "MUNIC_MOV": "230440",
            "PROC_REA": "0408050160",
            "VAL_TOT": "4200.00",
            "ano": "2026",
            "mes": "05",
            "uf": "CE"
        },
        {
            "N_AIH": "1234567890125",
            "CNES": "2077485",
            "MUNIC_MOV": "230440",
            "PROC_REA": "0301010072",
            "VAL_TOT": "350.00",
            "ano": "2026",
            "mes": "05",
            "uf": "CE"
        }
    ])

    df_er = pd.DataFrame([
        {
            "N_AIH": "1234567890123",
            "CO_ERRO": "101",
            "DS_ERRO": "Incompatibilidade de procedimento com sexo do paciente",
            "ano": "2026",
            "mes": "05",
            "uf": "CE"
        },
        {
            "N_AIH": "1234567890124",
            "CO_ERRO": "204",
            "DS_ERRO": "Ultrapassou teto or?ament?rio mensal do CNES",
            "ano": "2026",
            "mes": "05",
            "uf": "CE"
        }
        # AIH 1234567890125 sem registro no ER (deve virar NAO_INFORMADO)
    ])

    sql_glosas = """
    SELECT
        md5(concat_ws('-', CAST(r.N_AIH AS VARCHAR), COALESCE(CAST(e.CO_ERRO AS VARCHAR), '000'), CAST(r.ano AS VARCHAR), CAST(r.mes AS VARCHAR), CAST(r.uf AS VARCHAR))) AS id_glosa_hospitalar,
        CAST(r.N_AIH AS VARCHAR) AS numero_aih,
        CAST(r.CNES AS VARCHAR) AS codigo_estabelecimento_cnes,
        CAST(r.MUNIC_MOV AS VARCHAR) AS codigo_municipio_hospital,
        CAST(r.PROC_REA AS VARCHAR) AS codigo_procedimento,
        TRY_CAST(r.VAL_TOT AS DOUBLE) AS valor_glosado_brl,
        COALESCE(TRY_CAST(e.CO_ERRO AS VARCHAR), 'NAO_INFORMADO') AS codigo_motivo_glosa,
        COALESCE(e.DS_ERRO, 'Motivo de Rejei??o N?o Especificado') AS descricao_motivo_glosa,
        'SIH_REJEICAO_SUS' AS tipo_origem_glosa,
        r.ano, r.mes, r.uf
    FROM df_rj r
    LEFT JOIN df_er e 
        ON r.N_AIH = e.N_AIH AND r.ano = e.ano AND r.mes = e.mes AND r.uf = e.uf
    """

    df_res = conn.execute(sql_glosas).df()
    conn.close()

    assert len(df_res) == 3
    assert df_res["id_glosa_hospitalar"].nunique() == len(df_res), "id_glosa_hospitalar deve ser estritamente ?nico."
    
    row1 = df_res[df_res["numero_aih"] == "1234567890123"].iloc[0]
    assert row1["codigo_motivo_glosa"] == "101"
    assert "Incompatibilidade" in row1["descricao_motivo_glosa"]
    assert row1["valor_glosado_brl"] == 1500.50

    row3 = df_res[df_res["numero_aih"] == "1234567890125"].iloc[0]
    assert row3["codigo_motivo_glosa"] == "NAO_INFORMADO"
    assert row3["descricao_motivo_glosa"] == "Motivo de Rejei??o N?o Especificado"


def test_build_dm_motivos_glosas_hospitalares():
    """Valida a agrega??o e ranking dos motivos reais de glosa hospitalar na camada Gold."""
    df_glosas = pd.DataFrame([
        {"numero_aih": "1", "codigo_motivo_glosa": "101", "descricao_motivo_glosa": "Erro 101", "valor_glosado_brl": 1000.0},
        {"numero_aih": "2", "codigo_motivo_glosa": "101", "descricao_motivo_glosa": "Erro 101", "valor_glosado_brl": 2000.0},
        {"numero_aih": "3", "codigo_motivo_glosa": "204", "descricao_motivo_glosa": "Erro 204", "valor_glosado_brl": 7000.0},
    ])

    df_mart = build_dm_motivos_glosas_hospitalares(df_glosas)
    assert len(df_mart) == 2
    # O maior valor total glosado (Erro 204 com 7000.0 = 70%) deve ser o primeiro do ranking
    assert df_mart.iloc[0]["codigo_motivo_glosa"] == "204"
    assert df_mart.iloc[0]["valor_total_glosado_brl"] == 7000.0
    assert df_mart.iloc[0]["percentual_valor_glosado_pct"] == 70.0

    # Erro 101 tem 2 AIHs e 3000.0 = 30%
    assert df_mart.iloc[1]["codigo_motivo_glosa"] == "101"
    assert df_mart.iloc[1]["total_aih_glosadas"] == 2
    assert df_mart.iloc[1]["percentual_valor_glosado_pct"] == 30.0
