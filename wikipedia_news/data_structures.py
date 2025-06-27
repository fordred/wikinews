from dataclasses import dataclass
from datetime import datetime

@dataclass
class ProcessingItem:
    mode: str
    month_datetime: datetime
    retry_count: int = 0

@dataclass
class DailyEvent:
    date: datetime
    content: str
    source: str
