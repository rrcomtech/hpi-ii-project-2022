import logging
import os
from pathlib import Path

import click

from rights_bafin_crawler.constant import Letter
from union_generator.union_extractor import UnionExtractor

logging.basicConfig(
    level=os.environ.get("LOGLEVEL", "INFO"), format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
)
log = logging.getLogger(__name__)

@click.command()
@click.option("-p", "--csv_path", type=Path, help="The path if you want to store the output in a csv file.")
def run(csv_path: Path = ""):
    UnionExtractor(csv_path).extract()



if __name__ == "__main__":
    run()
