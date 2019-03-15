from LUCIS_AnalysisFunctions import ProximityConservationParksWater
from settings import sf_parcel_data, cell_size


def urbsfConservationParksWater(inputgeodfsql):
    lu = 'sf'
    openwater_suit = "SELECT lower, upper, suit_score FROM suit_urbmodel_byinterval " \
                     "WHERE model = 'openwater' AND land_use_type = '{}'".format(lu)
    consparkow_wgt = "SELECT class, weight FROM wgt_urbmodel " \
                     "WHERE model = 'conservation' AND land_use_type = '{}'".format(lu)
    return ProximityConservationParksWater(inputgeodfsql, sf_parcel_data, cell_size,
                                           openwater_suit, consparkow_wgt, lu)

