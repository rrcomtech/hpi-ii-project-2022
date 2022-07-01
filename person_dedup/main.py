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
    msg = PersonConsumer().consume()
    log.info(msg)
    msg_bafin_persons = msg["bafin-persons"]
    msg_rb_persons = msg["rb-persons"]
    log.info(msg_bafin_persons[0].issuer)
    log.info(msg_rb_persons[0].issuer)


if __name__ == "__main__":
    run()
