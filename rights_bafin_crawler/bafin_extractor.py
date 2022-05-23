import logging
import csv
from pathlib import Path
from time import sleep

import requests

import pandas as pd
from io import StringIO

from build.gen.bakdata.bafin.v1.bafin_pb2 import Bafin_general, Bafin_detail
from bafin_producer import BafinProducer
from rights_bafin_crawler.constant import Letter

log = logging.getLogger(__name__)


class BafinExtractor:
    def __init__(self, detail: bool, csv_path: Path):
        self.crawl_detail = detail
        self.csv_path = csv_path
        self.producer = BafinProducer()
        

    def extract(self):
        if self.csv_path != "":
            csv_file = open(self.csv_path, 'w')
            writer = csv.writer(csv_file)
            if self.crawl_detail:
                header = ["Issuer BaFin-Id", "Issuer", "Issuer Domicile", "Issuer Country", "Reportable BaFin-Id", "Reportable", "Reportable Domicile", "Reportable Country", "Rights §33 & §34", "Rights §38", "Rights §39", "Publishing Date"]
            else:
                header = ["Issuer BaFin-Id", "Issuer", "Issuer Domicile", "Issuer Country"]
            writer.writerow(header)
        
        for letter in Letter:
            try:
                log.info(f"Sending General Request for letter: {letter}")
                general_text = self.send_meta_request(letter)
                general_df = pd.read_csv(StringIO(general_text), sep=";", header=0)

                for _ , general_row in general_df.iterrows():
                    bafin_general = Bafin_general()
                    bafin_general.issuer_id    = general_row['BaFin-Id']
                    bafin_general.issuer       = general_row['Emittent']
                    bafin_general.domicile     = general_row['Sitz']
                    bafin_general.country      = general_row['Land']

                    if self.crawl_detail:

                        try:
                            log.info(f"Sending Detail Request for company: {general_row['Emittent']} (ID: {general_row['BaFin-Id']})")
                            detail_text = self.send_detail_request(general_row['BaFin-Id'])
                            detail_df = pd.read_csv(StringIO(detail_text), sep=";", header=0, na_filter=False)
                            if self.csv_path != "":
                                if self.crawl_detail:
                                    row = [general_row['BaFin-Id'], general_row['Emittent'], general_row['Sitz'], general_row['Land'], "", "", "", "", "", "", "", ""]
                                else:
                                    row = [general_row['BaFin-Id'], general_row['Emittent'], general_row['Sitz'], general_row['Land']]
                                writer.writerow(row)

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
                                
                                if self.csv_path != "":
                                    writer.writerow([
                                        "", "", "", "",
                                        detail_row['BaFin-Id'],
                                        detail_row['Meldepflichtiger / Tochterunternehmen (T)'],
                                        detail_row['Sitz oder Ort'],
                                        detail_row['Land'],
                                        detail_row['§§ 33, 34 WpHG (Prozent)'],
                                        detail_row['§ 38 WpHG (Prozent)'],
                                        detail_row['§ 39 WpHG (Prozent)'],
                                        detail_row['Veröffentlichung gemäß § 40 Abs.1 WpHG'],
                                    ])
                                bafin_general.bafin_detail.append(bafin_detail)

                        except Exception as ex:
                            log.error(f"Skipping Company {general_row['Emittent']}")
                            log.error(f"Cause: {ex}")
                            continue
                    
                    
                    
                    self.producer.produce_to_topic(bafin_general=bafin_general)
                    log.debug(bafin_general)

            except Exception as ex:
                log.error(f"Skipping Letter {letter}")
                log.error(f"Cause: {ex}")
                continue
        csv_file.close()
        exit(0)

    def send_meta_request(self, letter : str) -> str:
        url = f"https://portal.mvp.bafin.de/database/AnteileInfo/suche.do?nameAktiengesellschaftButton=Suche+Emittent&d-7004401-e=1&6578706f7274=1&nameMeldepflichtiger=&nameAktiengesellschaft={letter}"
        # For graceful crawling! Remove this at your own risk!
        sleep(0.5)
        return requests.get(url=url).text
    
    def send_detail_request(self, bafin_id : int) -> str:
        url = f"https://portal.mvp.bafin.de/database/AnteileInfo/aktiengesellschaft.do?d-3611442-e=1&cmd=zeigeAktiengesellschaft&id={bafin_id}&6578706f7274=1"
        # For graceful crawling! Remove this at your own risk!
        sleep(1.5)
        return requests.get(url=url).text

