from LUCIS_AnalysisFunctions import HabitatReclassify


def conHabitatManagedArea(inputgeodfsql):
    congfchab_suit = "SELECT class, suit_score FROM suit_conmodel_bycase " \
                     "WHERE model = 'conservation gfchab'"
    return HabitatReclassify(inputgeodfsql, congfchab_suit)
