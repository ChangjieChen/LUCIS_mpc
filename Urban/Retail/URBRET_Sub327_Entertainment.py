from LUCIS_AnalysisFunctions import ProximityEntertainment
from settings import ret_parcel_data, cell_size


def urbretEntertainment(inputgeodfsql):
    lu = 'ret'
    entertainment_wgt = "SELECT class, weight FROM wgt_urbmodel " \
                        "WHERE model = 'entertainment' AND land_use_type = '{}'".format(lu)
    return ProximityEntertainment(inputgeodfsql, ret_parcel_data, cell_size, entertainment_wgt, lu)


