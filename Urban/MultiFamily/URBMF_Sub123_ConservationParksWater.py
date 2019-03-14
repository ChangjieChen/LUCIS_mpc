from LUCIS_AnalysisFunctions import ProximityConservationParksWater
from settings import mf_parcel_data, cell_size


def urbmfConservationParksWater(inputgeodfsql):
    lu = 'mf'
    openwater_suit = "SELECT lower, upper, suit_score FROM suit_urbmodel_byinterval " \
                     "WHERE model = 'openwater' AND land_use_type = '{}'".format(lu)
    consparkow_wgt = "SELECT class, weight FROM wgt_urbmodel " \
                     "WHERE model = 'conservation' AND land_use_type = '{}'".format(lu)
    return ProximityConservationParksWater(inputgeodfsql, mf_parcel_data, cell_size,
                                           openwater_suit, consparkow_wgt, lu)
