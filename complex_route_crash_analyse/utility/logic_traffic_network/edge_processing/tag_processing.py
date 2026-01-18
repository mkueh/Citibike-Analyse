import pandas as pd

class TagProcessing:
    
    @staticmethod
    def normalize_tags(value) -> list[str]:
        if value is None:
            return []
        if isinstance(value, (list, tuple, set)):
            items = list(value)
        elif hasattr(pd, "isna") and pd.isna(value):
            return []
        else:
            items = [value]
        normalized: list[str] = []
        for item in items:
            if item is None:
                continue
            if hasattr(pd, "isna") and not isinstance(item, (list, tuple, set)) and pd.isna(item):
                continue
            if isinstance(item, (list, tuple, set)):
                normalized.extend([str(x) for x in item if x is not None])
            else:
                normalized.append(str(item))
        return normalized