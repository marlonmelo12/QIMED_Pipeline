import pandas as pd
from src.validators.base import BaseValidator, ValidationResult

class FhirValidator(BaseValidator):
    def validate(self, df: pd.DataFrame) -> ValidationResult:
        required_cols = ["resourceType", "id"]
        
        missing_cols = [c for c in required_cols if c not in df.columns]
        if missing_cols:
            return ValidationResult(pd.DataFrame(columns=df.columns), df.copy(), {"reason": f"Missing columns: {missing_cols}"})
            
        valid_mask = df["resourceType"].isin(["Patient", "Encounter", "Observation", "Condition", "Procedure", "Organization"])
        
        valid_df = df[valid_mask].copy()
        rejected_df = df[~valid_mask].copy()
        
        return ValidationResult(valid_df, rejected_df, {"valid_count": len(valid_df), "rejected_count": len(rejected_df)})
