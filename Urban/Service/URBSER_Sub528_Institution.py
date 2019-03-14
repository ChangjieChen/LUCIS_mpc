from LUCIS_AnalysisFunctions import ProximityInstitution
from settings import ser_parcel_data, cell_size


def urbserInstitution(inputgeodfsql):
    lu = 'ser'
    institution_wgt = "SELECT class, weight FROM wgt_urbmodel " \
                      "WHERE model = 'institutional' AND land_use_type = '{}'".format(lu)
    return ProximityInstitution(inputgeodfsql, ser_parcel_data, cell_size, institution_wgt, lu)
