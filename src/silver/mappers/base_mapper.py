"""
Base semantic mapper interface and CanonicalDataset container for Silver transformations.
"""
import abc
from dataclasses import dataclass, field
from typing import Dict, Any, Optional
import pandas as pd


@dataclass
class CanonicalDataset:
    """
    Standardized container of canonical Silver tabular entities.
    """
    dim_patients: pd.DataFrame = field(default_factory=lambda: pd.DataFrame())
    dim_organizations: pd.DataFrame = field(default_factory=lambda: pd.DataFrame())
    dim_health_plans: pd.DataFrame = field(default_factory=lambda: pd.DataFrame())
    fct_encounters: pd.DataFrame = field(default_factory=lambda: pd.DataFrame())
    fct_conditions: pd.DataFrame = field(default_factory=lambda: pd.DataFrame())
    fct_procedures: pd.DataFrame = field(default_factory=lambda: pd.DataFrame())
    fct_referrals: pd.DataFrame = field(default_factory=lambda: pd.DataFrame())
    metadata: Dict[str, Any] = field(default_factory=dict)

    def summary(self) -> Dict[str, int]:
        return {
            "patients": len(self.dim_patients),
            "organizations": len(self.dim_organizations),
            "health_plans": len(self.dim_health_plans),
            "encounters": len(self.fct_encounters),
            "conditions": len(self.fct_conditions),
            "procedures": len(self.fct_procedures),
            "referrals": len(self.fct_referrals),
        }


class BaseSemanticMapper(abc.ABC):
    """
    Abstract interface for mapping raw/Bronze source schemas to the canonical Silver data model.
    """

    @abc.abstractmethod
    def map_to_canonical(self, df: pd.DataFrame, source_metadata: Optional[Dict[str, Any]] = None) -> CanonicalDataset:
        """
        Transforms raw data into normalized canonical entities.
        """
        pass
