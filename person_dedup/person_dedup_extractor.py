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
                similarity = distance.get_jaro_distance(person, neighbour, winkler = True, scaling = 0.1)
                similarity_array.append(similarity)
                if similarity >= TRESHOLD:
                    duplicate_indices.append(i)
                    duplicate_indices.append(i + j + 1)

        for i, index in enumerate(sorted_indices):
            if i not in duplicate_indices:
                msg = None
                if index < len(msg_bafin_persons):
                    msg = msg_bafin_persons[index]
                else:
                    msg = msg_rb_persons[index - len(msg_bafin_persons)]
                person = self.personFromMessage(msg)
                self.dedup_producer.produce_to_topic(person, hash(person.firstname.lower() + person.lastname.lower()))

        print(duplicate_indices)

        duplicate_families = []
        for i, duplicate_index in enumerate(duplicate_indices):
            if i % 2 == 1 or duplicate_index < 0:
                continue
            duplicate_family = []
            duplicate_family.append(duplicate_index)
            duplicate_indices[i] = - duplicate_indices[i]
            if duplicate_indices[i + 1] > 0:
                duplicate_family.append(duplicate_indices[i + 1])
                duplicate_indices[i + 1] = - duplicate_indices[i + 1]
            for j, next_index in enumerate(duplicate_indices[i + 2:]):
                if next_index in duplicate_family:
                    if j % 2 == 0:
                        if duplicate_indices[i + j + 3] not in duplicate_family and duplicate_indices[i + j + 3] > 0:
                            duplicate_family.append(duplicate_indices[i + j + 3])
                            duplicate_indices[i + j + 3] = - duplicate_indices[i + j + 3]
                    else:
                        if duplicate_indices[i + j + 1] not in duplicate_family and duplicate_indices[i + j + 1] > 0:
                            duplicate_family.append(duplicate_indices[i + j + 1])
                            duplicate_indices[i + j + 1] = - duplicate_indices[i + j + 1]
            counter = 0
            for existing_duplicate_family in duplicate_families:
                if existing_duplicate_family[0] not in duplicate_family:
                    duplicate_families.append(duplicate_family)
                    counter += 1
            if counter == 0:
                duplicate_families.append(duplicate_family)
                
        print(duplicate_families)
        exit(0)

    def personFromMessage(self, msg):
        person = DeDup_Person()
        person.firstname = msg.firstname
        person.lastname = msg.lastname
        try:
            person.title = msg.title
        except:
            pass
        try:
            person.bafin_issuer = msg.bafin_issuer
        except:
            pass
        try:
            person.rights_33_34 = msg.rights_33_34
        except:
            pass
        try:
            person.rights_38 = msg.rights_38
        except:
            pass
        try:
            person.rights_39 = msg.rights_39
        except:
            pass
        try:
            person.bafin_reportable_id = msg.bafin_reportable_id
        except:
            pass
        try:
            person.bafin_issuer_id = msg.bafin_issuer_id
        except:
            pass
        try:
            person.publishing_date = msg.publishing_date
        except:
            pass
        try:
            person.rb_corporateName = msg.rb_corporateName
        except:
            pass
        try:
            person.birthdate = msg.birthdate
        except:
            pass
        try:
            person.city = msg.city
        except:
            pass
        try:
            person.rb_role = msg.rb_role
        except:
            pass
        try:
            person.rb_corporateID = msg.rb_corporateID
        except:
            pass
        return person
