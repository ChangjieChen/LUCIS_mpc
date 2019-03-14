from LUCIS_AnalysisFunctions import ProximityCommercial
from settings import ser_parcel_data, cell_size


def urbserCommercial(inputgeodfsql):
    lu = 'ser'
    commercial_wgt = "SELECT class, weight FROM wgt_urbmodel " \
                     "WHERE model = 'commercial' AND land_use_type = '{}'".format(lu)
    return ProximityCommercial(inputgeodfsql, ser_parcel_data, cell_size, commercial_wgt, lu)
