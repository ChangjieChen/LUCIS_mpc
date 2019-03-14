from LUCIS_AnalysisFunctions import ProximityExistingLandUse
from settings import ser_parcel_data


def urbserExistingLandUse(inputgeodfsql):
    lu = 'ser'
    return ProximityExistingLandUse(inputgeodfsql, ser_parcel_data, 1995, lu)
