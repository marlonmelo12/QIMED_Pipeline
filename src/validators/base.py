import abc
import pandas as pd
from dataclasses import dataclass
from typing import Dict, Any

@dataclass
class ValidationResult:
    valid_df: pd.DataFrame
    rejected_df: pd.DataFrame
    stats: Dict[str, Any]

class BaseValidator(abc.ABC):
    @abc.abstractmethod
    def validate(self, df: pd.DataFrame) -> ValidationResult:
        pass
