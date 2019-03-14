from LUCIS_AnalysisFunctions import ProximityInfrastructure
from settings import ret_parcel_data


def urbretInfrastructure(inputgeodfsql):
    lu = 'ret'
    retinfras_wgt = "SELECT class, weight FROM wgt_urbmodel " \
                    "WHERE model = 'infrastructure' AND land_use_type = '{}'".format(lu)
    return ProximityInfrastructure(inputgeodfsql, ret_parcel_data, retinfras_wgt, lu)

