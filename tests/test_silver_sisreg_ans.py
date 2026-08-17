"""
Testes Unitarios para os Mappers Semanticos de SISREG e ANS na Camada Silver.
"""
import pytest
import pandas as pd
from src.collectors.sisreg_collector import SisregCollector
from src.collectors.ans_collector import AnsCollector
from src.silver.mappers.sisreg_mapper import SisregMapper
from src.silver.mappers.ans_mapper import AnsMapper
from src.silver.pipeline import SilverTransformationPipeline

def test_sisreg_mapper_canonical_dataset():
    collector = SisregCollector(uf="AC", year=2025, month=1)
    df = collector.parse(collector.fetch())
    
    mapper = SisregMapper()
    canonical = mapper.map_to_canonical(df)
    
    assert not canonical.fct_referrals.empty
    assert len(canonical.fct_referrals) == len(df)
    assert "referral_id" in canonical.fct_referrals.columns
    assert "wait_time_days" in canonical.fct_referrals.columns
    assert "patient_master_id" in canonical.fct_referrals.columns
    assert "requested_procedure_name" in canonical.fct_referrals.columns
    assert "request_hospital_name" in canonical.fct_referrals.columns

def test_ans_mapper_canonical_dataset():
    collector = AnsCollector(uf="AC", year=2025)
    df = collector.parse(collector.fetch())
    
    mapper = AnsMapper()
    canonical = mapper.map_to_canonical(df)
    
    assert not canonical.dim_health_plans.empty
    assert len(canonical.dim_health_plans) == len(df)
    assert "plan_id" in canonical.dim_health_plans.columns
    assert "private_coverage_ratio_pct" in canonical.dim_health_plans.columns
    assert "active_beneficiaries" in canonical.dim_health_plans.columns
    assert "loss_ratio_pct" in canonical.dim_health_plans.columns

def test_silver_pipeline_with_sisreg_and_ans(tmp_path):
    pipeline = SilverTransformationPipeline(
        bronze_base_path=str(tmp_path / "bronze"),
        silver_base_path=str(tmp_path / "silver")
    )
    
    # 1. Transform SISREG
    sisreg_col = SisregCollector(uf="AC", year=2025, month=1)
    df_sisreg = sisreg_col.parse(sisreg_col.fetch())
    res_sisreg = pipeline.transform_dataframe(df_sisreg, source_type="sisreg_regulation")
    assert len(res_sisreg.fct_referrals) > 0
    
    # 2. Transform ANS
    ans_col = AnsCollector(uf="AC", year=2025)
    df_ans = ans_col.parse(ans_col.fetch())
    res_ans = pipeline.transform_dataframe(df_ans, source_type="ans_data")
    assert len(res_ans.dim_health_plans) > 0
