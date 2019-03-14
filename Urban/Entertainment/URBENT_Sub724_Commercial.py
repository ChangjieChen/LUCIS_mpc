from LUCIS_AnalysisFunctions import ProximityCommercial
from settings import ent_parcel_data, cell_size


def urbentCommercial(inputgeodfsql):
    lu = 'ent'
    commercial_wgt = "SELECT class, weight FROM wgt_urbmodel " \
                     "WHERE model = 'commercial' AND land_use_type = '{}'".format(lu)
    return ProximityCommercial(inputgeodfsql, ent_parcel_data, cell_size, commercial_wgt, lu)
