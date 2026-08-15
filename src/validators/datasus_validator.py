import pandas as pd
from src.validators.base import BaseValidator, ValidationResult
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

class DatasusValidator(BaseValidator):
    def __init__(self, subsystem: str = "SIH"):
        self.subsystem = subsystem.upper()

    def validate(self, df: pd.DataFrame) -> ValidationResult:
        if self.subsystem == "SIH":
            required_cols = ["N_AIH", "ANO_CMPT", "MES_CMPT", "PROC_REA"]
        elif self.subsystem == "SIA":
            # PA (Producao Ambulatorial) / APAC (Alta Complexidade)
            # Permite PA_PROC_ID ou AP_PRID ou PA_CODUNI ou AP_CODUF
            required_cols = ["PA_PROC_ID"] if "PA_PROC_ID" in df.columns else (["AP_PRID"] if "AP_PRID" in df.columns else ["PA_CODUNI"] if "PA_CODUNI" in df.columns else ["PA_MES_CMPT"] if "PA_MES_CMPT" in df.columns else ["PA_PROC_ID"])
        elif self.subsystem == "CNES":
            required_cols = ["CNES", "CODUFMUN"]
        elif self.subsystem == "SINAN":
            required_cols = ["NU_NOTIFIC", "DT_NOTIFIC", "ID_MUNICIP"]
        elif self.subsystem == "SISAB":
            required_cols = ["CO_MUNICIPIO_IBGE", "NU_COMPETENCIA"]
        else:
            required_cols = []
            
        missing_cols = [c for c in required_cols if c not in df.columns]
        if missing_cols:
            logger.warning(f"Validation failed for {self.subsystem}. Missing required columns: {missing_cols}")
            return ValidationResult(pd.DataFrame(columns=df.columns), df.copy(), {"reason": f"Missing columns: {missing_cols}"})
            
        valid_df = df.copy()
        rejected_df = pd.DataFrame(columns=df.columns)
        
        logger.info(f"Validation succeeded for {self.subsystem}. Valid records: {len(valid_df)}")
        return ValidationResult(valid_df, rejected_df, {"valid_count": len(valid_df), "rejected_count": len(rejected_df)})
