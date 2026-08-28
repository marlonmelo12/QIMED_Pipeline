"""
Testes unitários para os Data Marts da Camada Gold e Data Warehouse (DuckDB).
"""
import pytest
import pandas as pd
import numpy as np

from src.gold.models.kpi_glosas_auditoria import build_dm_glosas_auditoria
from src.gold.models.kpi_hospital_efficiency import build_dm_hospital_efficiency
from src.gold.models.kpi_patient_readmissions import build_dm_patient_readmissions
from src.gold.models.kpi_regulation_bottlenecks import build_dm_regulation_bottlenecks
from src.gold.models.kpi_icsap_prevention import build_dm_icsap_prevention
from src.dw.dw_manager import DataWarehouseManager


class TestGoldModels:
    """Testes para a geração dos Data Marts de Negócio da Camada Gold."""

    def test_dm_glosas_auditoria_calculation(self):
        """O Data Mart de Glosas deve calcular corretamente os valores glosados e a taxa percentual de glosa."""
        sample_sia = pd.DataFrame([
            {
                "PA_CODUNI": "1234567",
                "hospital_name": "Hospital Central",
                "PA_UFMUN": "230440",
                "municipality_name": "Fortaleza",
                "PA_QTDPRO": 2,
                "PA_QTDAPR": 1,
                "PA_VALPRO": 100.0,
                "PA_VALAPR": 60.0
            },
            {
                "PA_CODUNI": "1234567",
                "hospital_name": "Hospital Central",
                "PA_UFMUN": "230440",
                "municipality_name": "Fortaleza",
                "PA_QTDPRO": 1,
                "PA_QTDAPR": 0,
                "PA_VALPRO": 50.0,
                "PA_VALAPR": 0.0
            }
        ])

        dm = build_dm_glosas_auditoria(sample_sia)
        assert len(dm) == 1
        row = dm.iloc[0]
        assert row["total_procedimentos_faturados"] == 3
        assert row["total_procedimentos_aprovados"] == 1
        assert row["total_procedimentos_glosados"] == 2
        assert row["total_faturado_brl"] == 150.0
        assert row["total_aprovado_brl"] == 60.0
        assert row["total_glosado_brl"] == 90.0
        # Taxa de glosa: (90 / 150) * 100 = 60.0%
        assert row["taxa_glosa_pct"] == 60.0

    def test_dm_hospital_efficiency_calculation(self):
        """O Data Mart de Eficiência deve calcular média de permanência, custos e taxa de mortalidade."""
        sample_enc = pd.DataFrame([
            {
                "encounter_id": "enc1",
                "organization_id": "org_cnes_2479214",
                "hospital_name": "Hospital Messejana",
                "length_of_stay_days": 10,
                "total_cost_brl": 5000.0,
                "discharge_disposition": "discharged_alive"
            },
            {
                "encounter_id": "enc2",
                "organization_id": "org_cnes_2479214",
                "hospital_name": "Hospital Messejana",
                "length_of_stay_days": 6,
                "total_cost_brl": 3000.0,
                "discharge_disposition": "expired"
            }
        ])

        dm = build_dm_hospital_efficiency(sample_enc)
        assert len(dm) == 1
        row = dm.iloc[0]
        assert row["total_internacoes"] == 2
        assert row["total_dias_permanencia"] == 16
        assert row["tempo_medio_permanencia_dias"] == 8.0
        assert row["custo_total_brl"] == 8000.0
        assert row["custo_medio_internacao_brl"] == 4000.0
        assert row["obitos_hospitalares"] == 1
        assert row["taxa_mortalidade_pct"] == 50.0

    def test_dm_patient_readmissions_calculation(self):
        """O Data Mart de Readmissões deve identificar internações sucessivas em até 30 dias do mesmo paciente."""
        sample_enc = pd.DataFrame([
            {
                "patient_master_id": "mpi_paciente_1",
                "period_start": "2025-01-01",
                "period_end": "2025-01-05",
                "primary_diagnosis_code": "I21.9",
                "primary_diagnosis_name": "Infarto Agudo do Miocárdio",
                "primary_diagnosis_chapter": "Doenças do aparelho circulatório",
                "total_cost_brl": 4000.0
            },
            {
                "patient_master_id": "mpi_paciente_1",
                "period_start": "2025-01-20",  # 15 dias após alta anterior (Readmissão em 30d)
                "period_end": "2025-01-28",
                "primary_diagnosis_code": "I21.9",
                "primary_diagnosis_name": "Infarto Agudo do Miocárdio",
                "primary_diagnosis_chapter": "Doenças do aparelho circulatório",
                "total_cost_brl": 6000.0
            },
            {
                "patient_master_id": "mpi_paciente_2",
                "period_start": "2025-01-02",
                "period_end": "2025-01-06",
                "primary_diagnosis_code": "I21.9",
                "primary_diagnosis_name": "Infarto Agudo do Miocárdio",
                "primary_diagnosis_chapter": "Doenças do aparelho circulatório",
                "total_cost_brl": 3000.0
            }
        ])

        dm = build_dm_patient_readmissions(sample_enc)
        assert len(dm) >= 1
        row = dm[dm["cid10_code"] == "I21"].iloc[0]
        assert row["total_pacientes"] == 2
        assert row["total_internacoes"] == 3
        assert row["readmissoes_30_dias"] == 1
        assert row["taxa_readmissao_30_dias_pct"] == 33.33
        assert row["custo_total_readmissoes_brl"] == 6000.0

    def test_dm_regulation_bottlenecks_calculation(self):
        """O Data Mart de Regulação deve calcular tempos de fila e taxa de autorização."""
        sample_ref = pd.DataFrame([
            {
                "referral_id": "ref1",
                "municipality_name": "Fortaleza",
                "referral_type": "LEITO_UTI",
                "status": "AUTORIZADA",
                "wait_time_days": 2.5
            },
            {
                "referral_id": "ref2",
                "municipality_name": "Fortaleza",
                "referral_type": "LEITO_UTI",
                "status": "AGUARDANDO_FILA",
                "wait_time_days": 5.5
            }
        ])

        dm = build_dm_regulation_bottlenecks(sample_ref)
        assert len(dm) == 1
        row = dm.iloc[0]
        assert row["total_solicitacoes"] == 2
        assert row["solicitacoes_autorizadas"] == 1
        assert row["solicitacoes_pendentes_fila"] == 1
        assert row["taxa_autorizacao_pct"] == 50.0
        assert row["tempo_medio_espera_dias"] == 4.0

    def test_dm_icsap_prevention_calculation(self):
        """O Data Mart de ICSAP deve sinalizar corretamente internações evitáveis (Asma, Hipertensão, Diabetes)."""
        sample_enc = pd.DataFrame([
            {"municipality_name": "Fortaleza", "primary_diagnosis_code": "I10", "total_cost_brl": 1000.0}, # Hipertensão (ICSAP)
            {"municipality_name": "Fortaleza", "primary_diagnosis_code": "J45.0", "total_cost_brl": 1500.0}, # Asma (ICSAP)
            {"municipality_name": "Fortaleza", "primary_diagnosis_code": "S72.0", "total_cost_brl": 4000.0}, # Fratura (NÃO ICSAP)
        ])

        dm = build_dm_icsap_prevention(sample_enc)
        assert len(dm) == 1
        row = dm.iloc[0]
        assert row["total_internacoes"] == 3
        assert row["internacoes_icsap_evitaveis"] == 2
        assert row["taxa_icsap_pct"] == 66.67
        assert row["custo_icsap_brl"] == 2500.0


class TestDataWarehouseManager:
    """Testes para o motor colunar DuckDB do Data Warehouse."""

    def test_duckdb_in_memory_query(self):
        """O gerenciador do DW deve permitir queries SQL vetorizadas e registro de tabelas."""
        dw = DataWarehouseManager(db_path=":memory:")
        try:
            df_test = pd.DataFrame({
                "hospital": ["HGF", "Messejana"],
                "internacoes": [100, 80],
                "custo": [100000.0, 85000.0]
            })
            dw.register_table_from_df("dim_test", df_test)

            res = dw.query_df("SELECT SUM(internacoes) AS total_int, AVG(custo) AS media_custo FROM dim_test")
            assert res["total_int"].iloc[0] == 180
            assert res["media_custo"].iloc[0] == 92500.0
        finally:
            dw.close()
