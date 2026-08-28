"""
Testes Automatizados de Agrupamento Canônico dos 22 Capítulos da CID-10 (Task 10).
"""
import pytest
from src.silver.cid10_nacional import resolver_cid10_nacional
from src.silver.terminology import TerminologyService, CID10_CHAPTERS


def test_desambiguacao_letras_criticas_d_e_h():
    """Valida a separação estrita entre faixas que compartilham a mesma letra (D e H)."""
    # Letra D: D00-D48 (Neoplasias) vs D50-D89 (Doenças do sangue)
    desc_d48, cap_d48 = resolver_cid10_nacional("D48.9")
    assert "Neoplasias" in cap_d48, f"D48.9 deveria ser Neoplasias, obteve: {cap_d48}"

    desc_d50, cap_d50 = resolver_cid10_nacional("D50.0")
    assert "sangue" in cap_d50.lower(), f"D50.0 deveria ser Doenças do sangue, obteve: {cap_d50}"

    desc_d50_sem_ponto, cap_d50_sem_ponto = resolver_cid10_nacional("D50")
    assert "sangue" in cap_d50_sem_ponto.lower(), f"D50 deveria ser Doenças do sangue, obteve: {cap_d50_sem_ponto}"

    # Letra H: H00-H59 (Olhos/Oftalmo) vs H60-H95 (Ouvidos/Otorrino)
    desc_h10, cap_h10 = resolver_cid10_nacional("H10.1")
    assert "olho" in cap_h10.lower() or "oftalmologia" in cap_h10.lower(), f"H10.1 deveria ser Olhos, obteve: {cap_h10}"

    desc_h65, cap_h65 = resolver_cid10_nacional("H65.0")
    assert "ouvido" in cap_h65.lower() or "otorrino" in cap_h65.lower(), f"H65.0 deveria ser Ouvidos, obteve: {cap_h65}"


def test_cobertura_integral_22_capitulos():
    """Valida 1 código representativo para cada um dos 22 capítulos da CID-10 sem fallback indevido."""
    casos_teste = [
        ("A09", "Infecciosas"),
        ("C50", "Neoplasias"),
        ("D50", "sangue"),
        ("E11", "endócrinas"),
        ("F32", "mentais"),
        ("G40", "nervoso"),
        ("H10", "olho"),
        ("H65", "ouvido"),
        ("I21", "circulatório"),
        ("J18", "respiratório"),
        ("K35", "digestivo"),
        ("L03", "pele"),
        ("M54", "osteomuscular"),
        ("N39", "geniturinário"),
        ("O80", "Gravidez"),
        ("P07", "perinatal"),
        ("Q00", "congênitas"),
        ("R10", "Sintomas"),
        ("S72", "Lesões"),
        ("V01", "Causas externas"),
        ("Z30", "Fatores"),
        ("U07.1", "propósitos especiais"),
    ]

    for codigo, trecho_esperado in casos_teste:
        desc, cap = resolver_cid10_nacional(codigo)
        assert cap != "Outras Condições Clínicas", f"Código {codigo} caiu indevidamente em Outras Condições Clínicas."
        assert trecho_esperado.lower() in cap.lower(), f"Código {codigo}: esperado trecho '{trecho_esperado}', obteve capítulo '{cap}'."


def test_formatos_com_e_sem_ponto_e_case_insensitive():
    """Garante que variações de formatação e maiúsculas/minúsculas gerem a mesma categorização."""
    variacoes_d50 = ["D50.0", "D500", "d50.0", "d50", "D50"]
    capitulos_d50 = [resolver_cid10_nacional(v)[1] for v in variacoes_d50]
    
    assert len(set(capitulos_d50)) == 1, f"Divergência na classificação de D50: {capitulos_d50}"
    assert "sangue" in capitulos_d50[0].lower()


def test_terminology_service_capitulo_22():
    """Valida que TerminologyService reconhece o Capítulo XXII (U00-U99 / COVID-19)."""
    assert "XXII" in CID10_CHAPTERS
    assert CID10_CHAPTERS["XXII"][0] == "U00"
    assert CID10_CHAPTERS["XXII"][1] == "U99"
    assert "COVID-19" in CID10_CHAPTERS["XXII"][2]
    
    code, meta = TerminologyService.normalize_cid10("U07.1")
    assert meta["valid"] is True
    assert meta["chapter"] == "XXII"
    assert "COVID-19" in meta["chapter_description"]
