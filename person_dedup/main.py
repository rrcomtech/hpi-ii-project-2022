import logging
import os

import click

from person_dedup_extractor import PersonDedupExtractor


logging.basicConfig(
    level=os.environ.get("LOGLEVEL", "INFO"), format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
)
log = logging.getLogger(__name__)


@click.command()
def run():
    PersonDedupExtractor().extract()


if __name__ == "__main__":
    run()
