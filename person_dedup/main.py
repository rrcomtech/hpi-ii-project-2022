import logging
import os

import click

from person_consumer import PersonConsumer

logging.basicConfig(
    level=os.environ.get("LOGLEVEL", "INFO"), format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
)
log = logging.getLogger(__name__)


@click.command()
def run():
    PersonConsumer().consume()


if __name__ == "__main__":
    run()
