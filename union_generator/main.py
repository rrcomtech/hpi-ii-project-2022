import logging
import os
from pathlib import Path

import click

from rights_bafin_crawler.constant import Letter
from union_generator.union_consumer import UnionConsumer
from union_generator.union_extractor import UnionExtractor

logging.basicConfig(
    level=os.environ.get("LOGLEVEL", "INFO"), format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
)
log = logging.getLogger(__name__)

@click.command()
def run():
    extractor = UnionExtractor()
    extractor.extract()


if __name__ == "__main__":
    run()
