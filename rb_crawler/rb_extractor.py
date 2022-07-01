import logging
from time import sleep

import re

import requests
from parsel import Selector

from build.gen.bakdata.rb.v1.rb_corporate_pb2 import RB_Corporate, RB_Status
from build.gen.bakdata.rb.v1.rb_person_pb2 import RB_Person

from constant import TOPIC_RB_CORPORATE, TOPIC_RB_PERSON

from general.producer import Producer

log = logging.getLogger(__name__)

class RbExtractor:
    def __init__(self, start_rb_id: int, state: str):
        self.rb_id = start_rb_id
        self.state = state
        self.rb_corporate_producer = Producer(RB_Corporate, TOPIC_RB_CORPORATE)
        self.rb_person_producer = Producer(RB_Person, TOPIC_RB_PERSON)

    def extract(self):
        while True:
            try:
                log.info(f"Sending Request for: {self.rb_id} and state: {self.state}")
                text = self.send_request()
                if "Falsche Parameter" in text:
                    log.info("The end has reached")
                    break
                selector = Selector(text=text)
                corporate = RB_Corporate()
                corporate.rb_id = self.rb_id
                corporate.state = self.state
                corporate.reference_id = self.extract_company_reference_number(selector)
                event_type = selector.xpath("/html/body/font/table/tr[3]/td/text()").get()
                corporate.event_date = selector.xpath("/html/body/font/table/tr[4]/td/text()").get()
                corporate.id = f"{self.state}_{self.rb_id}"
                raw_text: str = selector.xpath("/html/body/font/table/tr[6]/td/text()").get()
                corporate.company_name = self.extract_company_name(raw_text)
                self.extract_persons(corporate, raw_text)
                self.handle_events(corporate, event_type, raw_text)
                self.rb_id = self.rb_id + 1
                log.debug(corporate)
            except Exception as ex:
                log.error(f"Skipping {self.rb_id} in state {self.state}")
                log.error(f"Cause: {ex}")
                self.rb_id = self.rb_id + 1
                continue
        exit(0)

    def send_request(self) -> str:
        url = f"https://www.handelsregisterbekanntmachungen.de/skripte/hrb.php?rb_id={self.rb_id}&land_abk={self.state}"
        # For graceful crawling! Remove this at your own risk!
        sleep(0.5)
        return requests.get(url=url).text

    @staticmethod
    def extract_company_reference_number(selector: Selector) -> str:
        return ((selector.xpath("/html/body/font/table/tr[1]/td/nobr/u/text()").get()).split(": ")[1]).strip()

    def handle_events(self, corporate, event_type, raw_text):
        if event_type == "Neueintragungen":
            self.handle_new_entries(corporate, raw_text)
        elif event_type == "Veränderungen":
            self.handle_changes(corporate, raw_text)
        elif event_type == "Löschungen":
            self.handle_deletes(corporate)

    def handle_new_entries(self, corporate: RB_Corporate, raw_text: str) -> RB_Corporate:
        log.debug(f"New company found: {corporate.id}")
        corporate.event_type = "create"
        corporate.information = raw_text
        corporate.status = RB_Status.STATUS_ACTIVE
        self.rb_corporate_producer.produce_to_topic(corporate, corporate.id)

    def handle_changes(self, corporate: RB_Corporate, raw_text: str):
        log.debug(f"Changes are made to company: {corporate.id}")
        corporate.event_type = "update"
        corporate.status = RB_Status.STATUS_ACTIVE
        corporate.information = raw_text
        self.rb_corporate_producer.produce_to_topic(corporate, corporate.id)

    def handle_deletes(self, corporate: RB_Corporate):
        log.debug(f"Company {corporate.id} is inactive")
        corporate.event_type = "delete"
        corporate.status = RB_Status.STATUS_INACTIVE
        self.rb_corporate_producer.produce_to_topic(corporate, corporate.id)

    def extract_company_name(self, raw_text: str):
        match = re.findall('^[^,]*', raw_text)[0]
        return match

    def extract_persons(self, corporate: RB_Corporate, raw_text: str):
        log.debug(f'Extract Persons for Company {corporate.id}')
        match = re.findall(' ([\w -]+), ([\w -]+), ([\w.\ -]+), \*(\d{2}.\d{2}.\d{4}), ([\w,\ -]+)', raw_text)
        for i in range(0, len(match)):
            person = RB_Person()
            person.firstname = match[i][0]
            person.lastname = match[i][1]
            person.city = match[i][2]
            person.birthdate = match[i][3]
            person.role = match[i][4]
            person.corporateName = corporate.company_name
            person.corporateID = corporate.state + str(corporate.rb_id)
            self.rb_person_producer.produce_to_topic(person, '#' + str(i) + '@' + person.corporateID)