from LUCIS_AnalysisFunctions import PhysicalFlooding


def urbindFlooding(inputgeodfsql):
    lu = 'ind'
    floodsoil_suit = "SELECT class, suit_score FROM suit_urbmodel_bycase " \
                     "WHERE model = 'flooding soil' AND land_use_type = '{}'".format(lu)
    floodgfchab_suit = "SELECT class, suit_score FROM suit_urbmodel_bycase " \
                       "WHERE model = 'flooding gfchab' AND land_use_type = '{}'".format(lu)
    flooding_wgt = "SELECT class, weight FROM wgt_urbmodel " \
                   "WHERE model = 'flooding' AND land_use_type = '{}'".format(lu)
    return PhysicalFlooding(inputgeodfsql, floodsoil_suit, floodgfchab_suit, flooding_wgt, lu)
