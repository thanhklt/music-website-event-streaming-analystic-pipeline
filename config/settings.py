"""Read and validate the pipeline's YAML configuration."""

from dataclasses import dataclass, fields
from pathlib import Path, PurePosixPath
from typing import Any
from urllib.parse import urlsplit

import yaml


DEFAULT_CONFIG_PATH = Path(__file__).resolve().with_name("config.yml")


@dataclass(frozen=True)
class Settings:
    """Configuration fields use the same names and types as config.yml."""

    KAFKA_ADDRESS: str
    KAFKA_PORT: int
    KAFKA_BOOTSTRAP_SERVERS: str
    KAFKA_CLUSTER_ID: str
    HDFS_URL: str
    HDFS_BASE_PATH: str
    HDFS_CHECKPOINT_PATH: str
    HDFS_NAMENODE_HTTP: str
    HDFS_OUTPUT_FORMAT: str
    HUE_PORT: int


def parse_config(config: dict[str, Any]) -> None:
    """Raise ValueError for missing/unknown keys, wrong types or invalid values."""
    
    expected = {}
    for field in fields(Settings):
        expected[field.name] = field.type
        
    missing = expected.keys() - config.keys()
    unknown = config.keys() - expected.keys()

    if missing:
        raise ValueError(f"Missing configuration keys: {', '.join(sorted(missing))}")
    if unknown:
        raise ValueError(f"Unknown configuration keys: {', '.join(sorted(map(str, unknown)))}")

    # Check kiểu dữ liệu
    for name, expected_type in expected.items():
        value = config[name]
        # bool is a subclass of int, but is not a valid port.
        if type(value) is not expected_type:
            raise ValueError(f"{name} must be {expected_type.__name__}")
        if expected_type is str and (not value.strip() or value != value.strip()):
            raise ValueError(f"{name} must be non-empty without surrounding whitespace")

def read_config() -> Settings:
    """Read YAML, validate its contents and return a Settings instance."""
    with Path(DEFAULT_CONFIG_PATH).open(encoding="utf-8") as config_file:
        try:
            config = yaml.safe_load(config_file)
        except yaml.YAMLError as exc:
            raise ValueError(f"Invalid YAML in configuration file: {path}") from exc

    if not isinstance(config, dict):
        raise ValueError("Configuration must be a YAML mapping of setting names to values")
    
    parse_config(config)
    return Settings(**config)

if __name__ == "__main__":
    print(read_config())
