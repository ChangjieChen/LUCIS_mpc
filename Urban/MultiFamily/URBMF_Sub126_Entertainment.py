from LUCIS_AnalysisFunctions import ProximityEntertainment
from settings import mf_parcel_data, cell_size


def urbmfEntertainment(inputgeodfsql):
    lu = 'mf'
    entertainment_wgt = "SELECT class, weight FROM wgt_urbmodel " \
                        "WHERE model = 'entertainment' AND land_use_type = '{}'".format(lu)
    return ProximityEntertainment(inputgeodfsql, mf_parcel_data, cell_size, entertainment_wgt, lu)
