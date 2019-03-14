from LUCIS_AnalysisFunctions import PresentLandUse


def urbcomActualUse(inputgeodfsql):
    lu = 'com'
    gfchab_suit = "SELECT class, suit_score FROM suit_urbmodel_bycase " \
                  "WHERE model = 'actual use gfchab' AND land_use_type = '{}'".format(lu)
    yrblt_suit = "SELECT lower, upper, suit_score FROM suit_urbmodel_byinterval " \
                 "WHERE model = 'year built' AND land_use_type = '{}'".format(lu)
    actualuse_wgt = "SELECT class, weight FROM wgt_urbmodel " \
                    "WHERE model = 'actualuse' AND land_use_type = '{}'".format(lu)
    return PresentLandUse(inputgeodfsql, gfchab_suit, yrblt_suit, actualuse_wgt, lu)
