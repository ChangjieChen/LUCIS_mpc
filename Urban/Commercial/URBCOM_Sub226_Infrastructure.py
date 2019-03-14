from LUCIS_AnalysisFunctions import ProximityInfrastructure
from settings import com_parcel_data


def urbcomInfrastructure(inputgeodfsql):
    lu = 'com'
    cominfras_wgt = "SELECT class, weight FROM wgt_urbmodel " \
                    "WHERE model = 'infrastructure' AND land_use_type = '{}'".format(lu)
    return ProximityInfrastructure(inputgeodfsql, com_parcel_data, cominfras_wgt, lu)

