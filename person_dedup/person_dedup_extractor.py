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
                person = self.dedupPersonFromMessages([msg])
                self.dedup_producer.produce_to_topic(person, hash(person.firstname.lower() + person.lastname.lower()))

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
            existing = False
            for existing_duplicate_family in duplicate_families:
                if set(existing_duplicate_family) & set(duplicate_family):
                    existing = True
            if not existing:
                duplicate_families.append(duplicate_family)
        print(duplicate_families)
        for family in duplicate_families:
            real_indices = []
            msgs = []
            for index in family:
                real_indices.append(sorted_indices[index])
            for index in real_indices:
                msg = None
                if index < len(msg_bafin_persons):
                    msgs.append(msg_bafin_persons[index])
                    msg = msg_bafin_persons[index]
                else:
                    msgs.append(msg_rb_persons[index - len(msg_bafin_persons)])
                    msg = msg_rb_persons[index - len(msg_bafin_persons)]
                dupPerson = self.dupPersonFromMessage(msg)
                self.dup_producer.produce_to_topic(dupPerson, hash(dupPerson.firstname.lower() + dupPerson.lastname.lower()))
            dedupPerson = self.dedupPersonFromMessages(msgs)
            self.dedup_producer.produce_to_topic(dedupPerson, hash(dedupPerson.firstname.lower() + dedupPerson.lastname.lower()))
        exit(0)

    def dedupPersonFromMessages(self, msgs):
        person = DeDup_Person()
        for msg in msgs:
            person.firstname = person.firstname if person.firstname and len(person.firstname) >= len(msg.firstname) else msg.firstname
            person.lastname = person.lastname if person.lastname and len(person.lastname) >= len(msg.lastname) else msg.lastname
            try:
                person.title = person.title if person.title and len(person.title) >= len(msg.title) else msg.title
            except:
                pass
            try:
                person.bafin_issuer = person.issuer if person.issuer and len(person.issuer) >= len(msg.issuer) else msg.issuer
            except:
                pass
            try:
                person.rights_33_34 = person.rights_33_34 if person.rights_33_34 and len(person.rights_33_34) >= len(msg.rights_33_34) else msg.rights_33_34
            except:
                pass
            try:
                person.rights_38 = person.rights_38 if person.rights_38 and len(person.rights_38) >= len(msg.rights_38) else msg.rights_38
            except:
                pass
            try:
                person.rights_39 = person.rights_39 if person.rights_39 and len(person.rights_39) >= len(msg.rights_39) else msg.rights_39
            except:
                pass
            try:
                person.bafin_reportable_id = person.reportable_id if person.reportable_id and len(person.reportable_id) >= len(msg.reportable_id) else msg.reportable_id
            except:
                pass
            try:
                person.bafin_issuer_id = person.issuer_id if person.issuer_id and len(person.issuer_id) >= len(msg.issuer_id) else msg.issuer_id
            except:
                pass
            try:
                person.publishing_date = person.publishing_date if person.publishing_date and len(person.publishing_date) >= len(msg.publishing_date) else msg.publishing_date
            except:
                pass
            try:
                person.rb_corporateName = person.corporateName if person.corporateName and len(person.corporateName) >= len(msg.corporateName) else msg.corporateName
            except:
                pass
            try:
                person.birthdate = person.birthdate if person.birthdate and len(person.birthdate) >= len(msg.birthdate) else msg.birthdate
            except:
                pass
            try:
                person.city = person.city if person.city and len(person.city) >= len(msg.city) else msg.city
            except:
                pass
            try:
                person.rb_role = person.role if person.role and len(person.role) >= len(msg.role) else msg.role
            except:
                pass
            try:
                person.rb_corporateID = person.corporateID if person.corporateID and len(person.corporateID) >= len(msg.corporateID) else msg.corporateID
            except:
                pass
        return person
    
    def dupPersonFromMessage(self, msg):
        person = Dup_Person()
        person.firstname = msg.firstname
        person.lastname = msg.lastname
        try:
            person.title = msg.title
        except:
            pass
        try:
            person.bafin_issuer = msg.issuer
        except:
            pass
        try:
            person.bafin_reportable_id = msg.reportable_id
        except:
            pass
        try:
            person.bafin_issuer_id = msg.issuer_id
        except:
            pass
        try:
            person.rb_corporateName = msg.corporateName
        except:
            pass
        try:
            person.rb_corporateID = msg.corporateID
        except:
            pass
        return person
