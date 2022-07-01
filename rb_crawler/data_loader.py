import os
from os.path import exists
import json

from rb_crawler.rb_extractor import RbExtractor

data_file_path = "rb_crawler/corporate-events-dump"

# Example:
# {
#   'id': 'be_558370',
#   'rb_id': 558370,
#   'state': 'be',
#   'reference_id': 'HRB 115392 B',
#   'event_date': '15.11.2019',
#   'event_type': 'update',
#   'status': 'STATUS_ACTIVE',
#   'information': 'HRB 115392 B: Westminster Immobilien GmbH, Berlin, Tiergartenstr. 10 c, 15711 KÃ¶nigs Wusterhausen. Ã„nderung zu Nr. 4: Infolge der ert
#                   eilten Vertretungsbefugnis, nun; GeschÃ¤ftsfÃ¼hrer: Neumann, Gunnar; mit der Befugnis die Gesellschaft allein zu vertreten\nmit der Befugnis RechtsgeschÃ¤fte
#                   mit sich selbst oder als Vertreter Dritter abzuschlieÃŸen'
# }
#

def main():
    if not check_for_data_file():
        return

    rb_extractor = RbExtractor(0, "be")
    file = open(data_file_path, "r", encoding="utf8")

    line = file.readline()
    while line:
        rb = json.loads(line)["_source"]
        line = file.readline()

        rb_extractor.extract_from_dataloader(
            rb_id=rb["rb_id"],
            state=rb["state"],
            reference_id=rb["reference_id"],
            event_date=rb["event_date"],
            event_type=translate_event_type(rb["event_type"]),
            text=rb["information"]
        )


def check_for_data_file():
    path = os.getcwd() + "/" + data_file_path
    file_exists = exists(path)

    if not file_exists:
        print("Could not find file 'corporate-events-dump' in the folder 'rb_crawler'")
        print("Please download it from here: https://owncloud.hpi.de/s/RBZRO1nhSSYRbyt")
        print("and unpack it.")
    return file_exists


def translate_event_type(type):
    if type == "update":
        return "Veränderungen"
    if type == "create":
        return "Neueintragungen"
    if type == "delete":
        return "Löschungen"
    return ""


if __name__ == "__main__":
    main()