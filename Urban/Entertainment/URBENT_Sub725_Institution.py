from LUCIS_AnalysisFunctions import ProximityInstitution
from settings import ent_parcel_data, cell_size


def urbentInstitution(inputgeodfsql):
    lu = 'ent'
    institution_wgt = "SELECT class, weight FROM wgt_urbmodel " \
                      "WHERE model = 'institutional' AND land_use_type = '{}'".format(lu)
    return ProximityInstitution(inputgeodfsql, ent_parcel_data, cell_size, institution_wgt, lu)
