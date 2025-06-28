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

@dataclass
class Config:
    base_wikipedia_url: str
    default_output_dir: str
    log_file: str
    retry_max_attempts: int
    retry_base_wait_seconds: int
    min_markdown_length_publish: int

@dataclass
class Arguments:
    verbose: bool
    local_html_dir: str | None
    output_dir: str
    workers: int | None
