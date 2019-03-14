from LUCIS_AnalysisFunctions import ProximityRetail
from settings import com_parcel_data, cell_size


def urbcomRetail(inputgeodfsql):
    lu = 'com'
    retail_wgt = "SELECT class, weight FROM wgt_urbmodel " \
                 "WHERE model = 'retail' AND land_use_type = '{}'".format(lu)
    return ProximityRetail(inputgeodfsql, com_parcel_data, cell_size, retail_wgt, lu)
