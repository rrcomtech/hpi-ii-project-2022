from cmath import nan
import logging
import math
from time import sleep

import requests
from parsel import Selector

import pandas as pd
from io import StringIO

from build.gen.bakdata.corporate.v1.bafin_pb2 import Bafin_meta, Bafin_detail
from bafin_producer import BafinProducer

log = logging.getLogger(__name__)


class BafinExtractor:
    def __init__(self, letter: str):
        self.letter = letter
        self.producer = BafinProducer()

    def extract(self):
        while True:
            try:
                log.info(f"Sending Meta Request for letter: {self.letter}")
                meta_text = self.send_meta_request()
                meta_df = pd.read_csv(StringIO(meta_text), sep=";", header=0)

                for _ , meta_row in meta_df.iterrows():
                    bafin_meta = Bafin_meta()
                    bafin_meta.issuer_id    = meta_row['BaFin-Id']
                    bafin_meta.issuer       = meta_row['Emittent']
                    bafin_meta.domicile     = meta_row['Sitz']
                    bafin_meta.country      = meta_row['Land']

                    try:
                        log.info(f"Sending Detail Request for company: {meta_row['Emittent']}")
                        detail_text = self.send_detail_request(meta_row['BaFin-Id'])
                        detail_df = pd.read_csv(StringIO(detail_text), sep=";", header=0, na_filter=False)
                        for _, detail_row in detail_df.iterrows():
                            bafin_detail = Bafin_detail()
                            bafin_detail.reportable_id          = detail_row['BaFin-Id']
                            bafin_detail.reportable             = detail_row['Meldepflichtiger / Tochterunternehmen (T)']
                            bafin_detail.reportable_domicile    = detail_row['Sitz oder Ort']
                            bafin_detail.reportable_country     = detail_row['Land']
                            bafin_detail.rights_33_34           = float(detail_row['§§ 33, 34 WpHG (Prozent)'].replace(',', '.') if detail_row['§§ 33, 34 WpHG (Prozent)'] != '' else 0)
                            bafin_detail.rights_38              = float(detail_row['§ 38 WpHG (Prozent)'].replace(',', '.') if detail_row['§ 38 WpHG (Prozent)'] != '' else 0)
                            bafin_detail.rights_39              = float(detail_row['§ 39 WpHG (Prozent)'].replace(',', '.') if detail_row['§ 39 WpHG (Prozent)'] != '' else 0)
                            bafin_detail.publishing_date        = detail_row['Veröffentlichung gemäß § 40 Abs.1 WpHG']
                            bafin_meta.bafin_detail.extend(bafin_detail)

                    except Exception as ex:
                        log.error(f"Skipping Company {meta_row['Emittent']}")
                        log.error(f"Cause: {ex}")
                        continue
                    
                    
                    
                    self.producer.produce_to_topic(bafin_meta=bafin_meta)
                    log.debug(bafin_meta)
                """
                Store result from Meta request: Read CSV and put all values into newly created bafin_meta schema for each value
                Start a new Detail request for every Bafin Id of the result from above and store it into bafin_detail schema
                Continue with next letter in alphabet until Z
                
                """
            except Exception as ex:
                log.error(f"Skipping Letter {self.letter}")
                log.error(f"Cause: {ex}")
                # go to next letter in alphabet
                continue
        exit(0)

    def send_meta_request(self) -> str:
        url = f"https://portal.mvp.bafin.de/database/AnteileInfo/suche.do?nameAktiengesellschaftButton=Suche+Emittent&d-7004401-e=1&6578706f7274=1&nameMeldepflichtiger=&nameAktiengesellschaft={self.letter}"
        # For graceful crawling! Remove this at your own risk!
        sleep(0.5)
        return requests.get(url=url).text
    
    def send_detail_request(self, bafin_id : int) -> str:
        url = f"https://portal.mvp.bafin.de/database/AnteileInfo/aktiengesellschaft.do?d-3611442-e=1&cmd=zeigeAktiengesellschaft&id={bafin_id}&6578706f7274=1"
        # For graceful crawling! Remove this at your own risk!
        sleep(1.5)
        return requests.get(url=url).text

