import sys
import time
import pytest
from pathlib import Path

ROOT = Path(__file__).parent.parent

sys.path.append(str(ROOT))


@pytest.fixture  # (scope="session")
def compose(docker_ip, docker_services):
    """Stupid simple way of making sure everything is up and running"""

    time.sleep(15)

    try:
        yield
    finally:
        print(docker_services._docker_compose.execute(f"logs ingestor"))
