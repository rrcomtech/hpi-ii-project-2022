import logging

from general.producer import Producer
from person_consumer import PersonConsumer

from build.gen.bakdata.union.v1.dup_person_pb2 import Dup_Person
from build.gen.bakdata.union.v1.dedup_person_pb2 import DeDup_Person

from constant import TOPIC_DUPPERSON, TOPIC_DEDUPPERSON, TRESHOLD

from pyjarowinkler import distance

log = logging.getLogger(__name__)


class PersonDedupExtractor:
    def __init__(self):
        self.dup_producer = Producer(Dup_Person, TOPIC_DUPPERSON)
        self.dedup_producer = Producer(DeDup_Person, TOPIC_DEDUPPERSON)
        

    def extract(self):
        cons = PersonConsumer()
        msgs = cons.consume()

        msg_bafin_persons = msgs["bafin-persons"]
        msg_rb_persons = msgs["rb-persons"]

        persons = []

        for person in msg_bafin_persons:
            persons.append(person.firstname.lower() + ' ' + person.lastname.lower())

        for person in msg_rb_persons:
            persons.append(person.firstname.lower() + ' ' + person.lastname.lower())

        sorted_indices = sorted(range(len(persons)), key=lambda k: persons[k])
        sorted_person_names = sorted(persons)

        duplicate_indices = []

        for i, person in enumerate(sorted_person_names):
            similarity_array = []
            for j, neighbour in enumerate(sorted_person_names[ i + 1 : min(i + 6, len(sorted_person_names) - 1)]):
                similarity = distance.get_jaro_distance(person, neighbour, winkler = False, scaling = 0.1)
                similarity_array.append(similarity)
                if similarity >= TRESHOLD:
                    duplicate_indices.append(i)
                    duplicate_indices.append(j)

        for i, index in enumerate(sorted_indices):
            if i not in duplicate_indices:

                msg = None
                if index < len(msg_bafin_persons):
                    msg = msg_bafin_persons[index]
                else:
                    msg = msg_rb_persons[index - len(msg_bafin_persons)]
                
                person = DeDup_Person()
                person.firstname = msg.firstname
                person.lastname = msg.lastname

                try:
                    person.title = msg.title if msg.title else ''
                    person.bafin_issuer = msg.bafin_issuer if msg.bafin_issuer else ''
                    person.rights_33_34 = msg.rights_33_34 if msg.rights_33_34 else ''
                    person.rights_38 = msg.rights_38 if msg.rights_38 else ''
                    person.rights_39 = msg.rights_39 if msg.rights_39 else ''
                    person.bafin_reportable_id = msg.bafin_reportable_id if msg.bafin_reportable_id else ''
                    person.bafin_issuer_id = msg.bafin_issuer_id if msg.bafin_issuer_id else ''
                    person.publishing_date = msg.publishing_date if msg.publishing_date else ''
                except AttributeError:
                    log.info('Attribute Error catched when producing Dedup Persons')
                
                try:
                    person.rb_corporateName = msg.rb_corporateName if msg.rb_corporateName else ''
                    person.birthdate = msg.birthdate if msg.birthdate else ''
                    person.city = msg.city if msg.city else ''
                    person.rb_role = msg.rb_role if msg.rb_role else ''
                    person.rb_corporateID = msg.rb_corporateID if msg.rb_corporateID else ''
                except AttributeError:
                    log.info('Attribute Error catched when producing Dedup Persons')
                
                self.dedup_producer.produce_to_topic(person, hash(person.firstname.lower() + person.lastname.lower()))

        exit(0)
