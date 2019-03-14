from LUCIS_AnalysisFunctions import PhysicalHazard


def urbsfHazard(inputgeodfsql):
    lu = 'sf'
    hazsuper_suit = "SELECT lower, upper, suit_score FROM suit_urbmodel_byinterval " \
                    "WHERE model = 'hazard superfund' AND land_use_type = '{}'".format(lu)
    hazstate_suit = "SELECT lower, upper, suit_score FROM suit_urbmodel_byinterval " \
                    "WHERE model = 'hazard statesites' AND land_use_type = '{}'".format(lu)
    hazard_wgt = "SELECT class, weight FROM wgt_urbmodel " \
                 "WHERE model = 'hazard' AND land_use_type = '{}'".format(lu)
    return PhysicalHazard(inputgeodfsql, hazsuper_suit, hazstate_suit, hazard_wgt, lu)
