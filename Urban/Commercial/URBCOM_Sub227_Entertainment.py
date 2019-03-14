from LUCIS_AnalysisFunctions import ProximityEntertainment
from settings import com_parcel_data, cell_size


def urbcomEntertainment(inputgeodfsql):
    lu = 'com'
    entertainment_wgt = "SELECT class, weight FROM wgt_urbmodel " \
                        "WHERE model = 'entertainment' AND land_use_type = '{}'".format(lu)
    return ProximityEntertainment(inputgeodfsql, com_parcel_data, cell_size, entertainment_wgt, lu)

