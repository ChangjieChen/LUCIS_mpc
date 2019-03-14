from LUCIS_AnalysisFunctions import ProximityExistingLandUse
from settings import ret_parcel_data


def urbretExistingLandUse(inputgeodfsql):
    lu = 'ret'
    return ProximityExistingLandUse(inputgeodfsql, ret_parcel_data, 1995, lu)

