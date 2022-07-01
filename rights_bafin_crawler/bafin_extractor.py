import logging
import re
from time import sleep

import requests

import pandas as pd
from io import StringIO

from build.gen.bakdata.bafin.v1.bafin_pb2 import Bafin_Issuer
from build.gen.bakdata.bafin.v1.bafin_corporate_pb2 import Bafin_Reportable_Corp
from build.gen.bakdata.bafin.v1.bafin_person_pb2 import Bafin_Reportable_Person

from constant import TOPIC_BAFIN, TOPIC_BAFIN_PERSON, TOPIC_BAFIN_CORPORATE

from general.producer import Producer


from rights_bafin_crawler.constant import Letter

log = logging.getLogger(__name__)


class BafinExtractor:
    def __init__(self, detail: bool):
        self.crawl_detail = detail
        self.bafin_issuer_producer = Producer(Bafin_Issuer, TOPIC_BAFIN)
        self.bafin_person_producer = Producer(Bafin_Reportable_Person, TOPIC_BAFIN_PERSON)
        self.bafin_corporate_producer = Producer(Bafin_Reportable_Corp, TOPIC_BAFIN_CORPORATE)
        

    def extract(self):
        
        for letter in Letter:
            try:
                log.info(f"Sending General Request for letter: {letter}")
                general_text = self.send_meta_request(letter)
                general_df = pd.read_csv(StringIO(general_text), sep=";", header=0)

                for _ , general_row in general_df.iterrows():
                    bafin_issuer = Bafin_Issuer()
                    bafin_issuer.issuer_id    = general_row['BaFin-Id']
                    bafin_issuer.issuer       = general_row['Emittent']
                    bafin_issuer.domicile     = general_row['Sitz']
                    bafin_issuer.country      = general_row['Land']

                    if self.crawl_detail:

                        try:
                            log.info(f"Sending Detail Request for company: {general_row['Emittent']} (ID: {general_row['BaFin-Id']})")
                            detail_text = self.send_detail_request(general_row['BaFin-Id'])
                            detail_df = pd.read_csv(StringIO(detail_text), sep=";", header=0, na_filter=False)
                            
                            for _, detail_row in detail_df.iterrows():

                                if not detail_row['Sitz oder Ort'] and not detail_row['Land']:
                                    bafin_reportable_person = Bafin_Reportable_Person()
                                    bafin_reportable_person.reportable_id       = detail_row['BaFin-Id']
                                    match = re.search('(.+), (.*\. )?(.+)', detail_row['Meldepflichtiger / Tochterunternehmen (T)'])
                                    bafin_reportable_person.title               = match.group(2) if match.group(2) else ''
                                    bafin_reportable_person.firstname           = match.group(3) if match.group(3) else ''
                                    bafin_reportable_person.lastname            = match.group(1) if match.group(1) else ''
                                    bafin_reportable_person.rights_33_34        = float(detail_row['§§ 33, 34 WpHG (Prozent)'].replace(',', '.') if detail_row['§§ 33, 34 WpHG (Prozent)'] != '' else 0)
                                    bafin_reportable_person.rights_38           = float(detail_row['§ 38 WpHG (Prozent)'].replace(',', '.') if detail_row['§ 38 WpHG (Prozent)'] != '' else 0)
                                    bafin_reportable_person.rights_39           = float(detail_row['§ 39 WpHG (Prozent)'].replace(',', '.') if detail_row['§ 39 WpHG (Prozent)'] != '' else 0)
                                    bafin_reportable_person.publishing_date     = detail_row['Veröffentlichung gemäß § 40 Abs.1 WpHG']
                                    bafin_reportable_person.issuer              = general_row['Emittent']
                                    bafin_reportable_person.issuer_id           = general_row['BaFin-Id']
                                    self.bafin_person_producer.produce_to_topic(bafin_reportable_person, bafin_reportable_person.reportable_id)
                                else:
                                    bafin_reportable_corp = Bafin_Reportable_Corp()
                                    bafin_reportable_corp.reportable_id         = detail_row['BaFin-Id']
                                    bafin_reportable_corp.reportable            = detail_row['Meldepflichtiger / Tochterunternehmen (T)']
                                    bafin_reportable_corp.reportable_domicile   = detail_row['Sitz oder Ort']
                                    bafin_reportable_corp.reportable_country    = detail_row['Land']
                                    bafin_reportable_corp.rights_33_34          = float(detail_row['§§ 33, 34 WpHG (Prozent)'].replace(',', '.') if detail_row['§§ 33, 34 WpHG (Prozent)'] != '' else 0)
                                    bafin_reportable_corp.rights_38             = float(detail_row['§ 38 WpHG (Prozent)'].replace(',', '.') if detail_row['§ 38 WpHG (Prozent)'] != '' else 0)
                                    bafin_reportable_corp.rights_39             = float(detail_row['§ 39 WpHG (Prozent)'].replace(',', '.') if detail_row['§ 39 WpHG (Prozent)'] != '' else 0)
                                    bafin_reportable_corp.publishing_date       = detail_row['Veröffentlichung gemäß § 40 Abs.1 WpHG']
                                    bafin_reportable_corp.issuer                = general_row['Emittent']
                                    bafin_reportable_corp.issuer_id             = general_row['BaFin-Id']
                                    self.bafin_corporate_producer.produce_to_topic(bafin_reportable_corp, bafin_reportable_corp.reportable_id)

                        except Exception as ex:
                            log.error(f"Skipping Company {general_row['Emittent']}")
                            log.error(f"Cause: {ex}")
                            continue
                    
                    self.bafin_issuer_producer.produce_to_topic(bafin_issuer, bafin_issuer.issuer_id)
                    log.debug(bafin_issuer)

            except Exception as ex:
                log.error(f"Skipping Letter {letter}")
                log.error(f"Cause: {ex}")
                continue
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

