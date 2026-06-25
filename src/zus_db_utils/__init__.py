"""zus_db_utils — zapis zagregowanych danych z pipeline'ow do roznych backendow."""

from zus_db_utils.core import AggReader, AggWriter
from zus_db_utils.logging_config import configure_file_logging

__version__ = "0.4.0"

__all__ = ["AggReader", "AggWriter", "configure_file_logging", "__version__"]
