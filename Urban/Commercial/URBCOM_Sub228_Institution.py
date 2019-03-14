from LUCIS_AnalysisFunctions import ProximityInstitution
from settings import com_parcel_data, cell_size


def urbcomInstitution(inputgeodfsql):
    lu = 'com'
    institution_wgt = "SELECT class, weight FROM wgt_urbmodel " \
                      "WHERE model = 'institutional' AND land_use_type = '{}'".format(lu)
    return ProximityInstitution(inputgeodfsql, com_parcel_data, cell_size, institution_wgt, lu)
