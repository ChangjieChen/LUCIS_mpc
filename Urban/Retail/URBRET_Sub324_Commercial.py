from LUCIS_AnalysisFunctions import ProximityCommercial
from settings import ret_parcel_data, cell_size


def urbretCommercial(inputgeodfsql):
    lu = 'ret'
    commercial_wgt = "SELECT class, weight FROM wgt_urbmodel " \
                     "WHERE model = 'commercial' AND land_use_type = '{}'".format(lu)
    return ProximityCommercial(inputgeodfsql, ret_parcel_data, cell_size, commercial_wgt, lu)

