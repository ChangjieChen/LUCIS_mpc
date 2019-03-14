from LUCIS_AnalysisFunctions import ProximityInfrastructure
from settings import sf_parcel_data


def urbsfInfrastructure(inputgeodfsql):
    lu = 'sf'
    sfinfras_wgt = "SELECT class, weight FROM wgt_urbmodel " \
                   "WHERE model = 'infrastructure' AND land_use_type = '{}'".format(lu)
    return ProximityInfrastructure(inputgeodfsql, sf_parcel_data, sfinfras_wgt, lu)
