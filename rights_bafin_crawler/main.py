import logging
import os
from pathlib import Path

import click

from rights_bafin_crawler.constant import Letter
from rights_bafin_crawler.bafin_extractor import BafinExtractor

logging.basicConfig(
    level=os.environ.get("LOGLEVEL", "INFO"), format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
)
log = logging.getLogger(__name__)

@click.command()
@click.option("-d", "--detail", type=bool, help="Indicate whether you want to also crawl the detail view.")
@click.option("-p", "--csv_path", type=Path, help="The path if you want to store the output in a csv file.")
def run(detail: bool = False, csv_path: Path = ""):
    BafinExtractor(detail, csv_path).extract()



if __name__ == "__main__":
    run()
