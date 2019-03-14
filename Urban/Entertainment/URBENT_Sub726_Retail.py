from LUCIS_AnalysisFunctions import ProximityRetail
from settings import ent_parcel_data, cell_size


def urbentRetail(inputgeodfsql):
    lu = 'ent'
    retail_wgt = "SELECT class, weight FROM wgt_urbmodel " \
                 "WHERE model = 'retail' AND land_use_type = '{}'".format(lu)
    return ProximityRetail(inputgeodfsql, ent_parcel_data, cell_size, retail_wgt, lu)
