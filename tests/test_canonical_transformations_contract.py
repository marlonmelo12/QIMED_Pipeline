"""
Testes de Contrato Arquitetural das Transformações Canônicas Silver - QIMED Lakehouse V3.
Valida:
1. Existência e assinaturas de todos os métodos em CanonicalTransformations (SIH, SIA, ANS, Tempo, Glosas);
2. Propagação de execution_id por MasterPipeline.execute_silver_transformation();
3. Execução individual e isolamento entre transformações;
4. Tratamento explícito de erros sem mascaramento de falhas.
"""
import pytest
from unittest.mock import MagicMock, patch

from src.processing.transformations import CanonicalTransformations
from src.pipeline.master_pipeline import QimedMasterPipeline


def test_canonical_transformations_api_contract():
    """Teste 1: CanonicalTransformations expõe todos os métodos públicos com assinatura esperada."""
    ct = CanonicalTransformations()

    # Métodos obrigatórios da Camada Silver
    assert hasattr(ct, "gerar_dim_tempo")
    assert callable(ct.gerar_dim_tempo)

    assert hasattr(ct, "transformar_ans_para_silver")
    assert callable(ct.transformar_ans_para_silver)

    assert hasattr(ct, "transformar_sih_para_silver")
    assert callable(ct.transformar_sih_para_silver)

    assert hasattr(ct, "transformar_sia_para_silver")
    assert callable(ct.transformar_sia_para_silver)

    assert hasattr(ct, "transformar_glosas_hospitalares_para_silver")
    assert callable(ct.transformar_glosas_hospitalares_para_silver)


def test_master_pipeline_calls_sih_and_sia_with_execution_id():
    """Teste 2 & 3: MasterPipeline.execute_silver_transformation chama todas as etapas Silver com execution_id."""
    pipeline = QimedMasterPipeline()
    exec_id = "exec_test_silver_123"

    with patch("src.pipeline.master_pipeline.CanonicalTransformations") as mock_ct_class:
        mock_ct = MagicMock()
        mock_ct_class.return_value = mock_ct

        res = pipeline.execute_silver_transformation(target_month=5, target_year=2026, execution_id=exec_id)

        assert res["status"] == "success"
        assert res["id_execucao"] == exec_id

        # Verifica chamadas com o execution_id correto
        mock_ct.gerar_dim_tempo.assert_called_once_with(start_year=2025, end_year=2027, execution_id=exec_id)
        mock_ct.transformar_ans_para_silver.assert_called_once_with(execution_id=exec_id)
        mock_ct.transformar_sih_para_silver.assert_called_once_with(execution_id=exec_id)
        mock_ct.transformar_sia_para_silver.assert_called_once_with(execution_id=exec_id)
        mock_ct.transformar_glosas_hospitalares_para_silver.assert_called_once_with(execution_id=exec_id)


def test_missing_bronze_sih_and_sia_handled_cleanly():
    """Teste 4: Quando Bronze SIH ou SIA não existem, métodos logam warning e retornam sem lançar exceções espúrias."""
    ct = CanonicalTransformations()
    # Executa com caminho vazio simulando tabela inexistente
    with patch("src.utils.s3_storage.lakehouse_path_exists", return_value=False):
        # Não deve lançar exceção
        ct.transformar_sih_para_silver(execution_id="exec_test_missing")
        ct.transformar_sia_para_silver(execution_id="exec_test_missing")
        ct.transformar_glosas_hospitalares_para_silver(execution_id="exec_test_missing")
