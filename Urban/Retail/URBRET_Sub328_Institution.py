from LUCIS_AnalysisFunctions import ProximityInstitution
from settings import ret_parcel_data, cell_size


def urbretInstitution(inputgeodfsql):
    lu = 'ret'
    institution_wgt = "SELECT class, weight FROM wgt_urbmodel " \
                      "WHERE model = 'institutional' AND land_use_type = '{}'".format(lu)
    return ProximityInstitution(inputgeodfsql, ret_parcel_data, cell_size, institution_wgt, lu)
