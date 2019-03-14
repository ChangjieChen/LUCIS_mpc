from LUCIS_AnalysisFunctions import PrimeFarmland


def agrcrpPrimeFarmland(inputgeodfsql):
    primefarm_suit = "SELECT class, suit_score FROM suit_agmodel_bycase " \
                     "WHERE model = 'nrcs prime farmland' AND land_use_type = 'row crops'"
    agparcel_suit = "SELECT class, suit_score FROM suit_agmodel_bycase " \
                    "WHERE model = 'parcel prime farmland' AND land_use_type = 'row crops'"
    return PrimeFarmland(inputgeodfsql, primefarm_suit, agparcel_suit)
