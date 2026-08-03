from pathlib import Path

import yaml


COMPOSE_FILE = Path(__file__).resolve().parents[1] / "docker-compose.yml"


def test_every_service_declares_a_restart_policy() -> None:
    services = yaml.safe_load(COMPOSE_FILE.read_text(encoding="utf-8"))["services"]
    missing = [
        name
        for name, config in services.items()
        if config.get("restart") != "unless-stopped"
    ]
    assert not missing, f"services without 'restart: unless-stopped': {missing}"
