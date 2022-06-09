import enum

TOPIC_RB_CORPORATE: str = "rb_corporate"
TOPIC_RB_PERSON: str = "rb_person"

class State(str, enum.Enum):
    BADEN_WUETTEMBERG = "bw"
    BAYERN = "by"
    BERLIN = "be"
    BRANDENBURG = "br"
    BREMEN = "hb"
    HAMBURG = "hh"
    HESSEN = "he"
    MECKLENBURG_VORPOMMERN = "mv"
    NIEDERSACHSEN = "ni"
    NORDRHEIN_WESTFALEN = "nw"
    RHEILAND_PFALZ = "rp"
    SAARLAND = "sl"
    SACHSEN = "sn"
    SACHSEN_ANHALT = "st"
    SCHLESWIG_HOLSTEIN = "sh"
    THUERINGEN = "th"
