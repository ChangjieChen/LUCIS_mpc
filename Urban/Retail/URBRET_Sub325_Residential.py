from LUCIS_AnalysisFunctions import ProximityResidential
from settings import ret_parcel_data


def urbretResidential(inputgeodfsql):
    lu = 'ret'
    return ProximityResidential(inputgeodfsql, ret_parcel_data, lu)

