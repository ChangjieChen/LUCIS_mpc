from LUCIS_AnalysisFunctions import PhysicalSoilQuality


def urbretSoilQuality(inputgeodfsql):
    lu = 'ret'
    soilcorr_wgt = "SELECT class, weight FROM wgt_urbmodel " \
                   "WHERE model = 'soil corrosion' AND land_use_type = '{}'".format(lu)
    soilcorr_suit = "SELECT class, suit_score FROM suit_urbmodel_bycase " \
                    "WHERE model = 'soil corrosion' AND land_use_type = '{}'".format(lu)
    soildrain_suit = "SELECT class, suit_score FROM suit_urbmodel_bycase " \
                     "WHERE model = 'soil drainage' AND land_use_type = '{}'".format(lu)
    return PhysicalSoilQuality(inputgeodfsql, soilcorr_wgt, soildrain_suit, soilcorr_suit, lu)

