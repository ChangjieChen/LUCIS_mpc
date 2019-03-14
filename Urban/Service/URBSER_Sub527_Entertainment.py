from LUCIS_AnalysisFunctions import ProximityEntertainment
from settings import ser_parcel_data, cell_size


def urbserEntertainment(inputgeodfsql):
    lu = 'ser'
    entertainment_wgt = "SELECT class, weight FROM wgt_urbmodel " \
                        "WHERE model = 'entertainment' AND land_use_type = '{}'".format(lu)
    return ProximityEntertainment(inputgeodfsql, ser_parcel_data, cell_size, entertainment_wgt, lu)
