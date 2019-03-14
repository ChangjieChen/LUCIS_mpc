from LUCIS_AnalysisFunctions import ProximityInfrastructure
from settings import ser_parcel_data


def urbserInfrastructure(inputgeodfsql):
    lu = 'ser'
    retinfras_wgt = "SELECT class, weight FROM wgt_urbmodel " \
                    "WHERE model = 'infrastructure' AND land_use_type = '{}'".format(lu)
    return ProximityInfrastructure(inputgeodfsql, ser_parcel_data, retinfras_wgt, lu)
