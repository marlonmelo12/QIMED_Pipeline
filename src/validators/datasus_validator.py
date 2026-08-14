import pandas as pd
from src.validators.base import BaseValidator, ValidationResult

class DatasusValidator(BaseValidator):
    def __init__(self, subsystem: str = "SIH"):
        self.subsystem = subsystem.upper()

    def validate(self, df: pd.DataFrame) -> ValidationResult:
        if self.subsystem == "SIH":
            required_cols = ["N_AIH", "ANO_CMPT", "MES_CMPT", "PROC_REA"]
        elif self.subsystem == "CNES":
            required_cols = ["CNES", "CODUFMUN"]
        else:
            required_cols = []
            
        missing_cols = [c for c in required_cols if c not in df.columns]
        if missing_cols:
            return ValidationResult(pd.DataFrame(columns=df.columns), df.copy(), {"reason": f"Missing columns: {missing_cols}"})
            
        valid_df = df.copy()
        rejected_df = pd.DataFrame(columns=df.columns)
        
        return ValidationResult(valid_df, rejected_df, {"valid_count": len(valid_df), "rejected_count": len(rejected_df)})
