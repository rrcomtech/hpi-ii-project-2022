import logging
import os

import click

from rights_bafin_crawler.constant import Letter
from rights_bafin_crawler.bafin_extractor import BafinExtractor

logging.basicConfig(
    level=os.environ.get("LOGLEVEL", "INFO"), format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
)
log = logging.getLogger(__name__)

@click.command()
@click.option("-l", "--letter", type=click.Choice(Letter), help="The letter to start crawling with.")
def run(letter: Letter):
    BafinExtractor(letter.value).extract()



if __name__ == "__main__":
    run()
