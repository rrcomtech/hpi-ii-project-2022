import logging
import os

import click

from name_dedup_extractor import Name_Dedup_Extractor

logging.basicConfig(
    level=os.environ.get("LOGLEVEL", "INFO"), format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
)
log = logging.getLogger(__name__)


@click.command()
def run():
    Name_Dedup_Extractor().extract()


if __name__ == "__main__":
    run()
