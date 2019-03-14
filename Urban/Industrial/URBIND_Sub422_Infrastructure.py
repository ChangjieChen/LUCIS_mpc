from LUCIS_AnalysisFunctions import ProximityInfrastructure
from settings import ret_parcel_data


def urbindInfrastructure(inputgeodfsql):
    lu = 'ind'
    infras_wgt = "SELECT class, weight FROM wgt_urbmodel " \
                 "WHERE model = 'infrastructure' AND land_use_type = '{}'".format(lu)
    return ProximityInfrastructure(inputgeodfsql, ret_parcel_data, infras_wgt, lu)

