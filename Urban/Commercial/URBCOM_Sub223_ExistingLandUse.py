from LUCIS_AnalysisFunctions import ProximityExistingLandUse
from settings import com_parcel_data


def urbcomExistingLandUse(inputgeodfsql):
    lu = 'com'
    return ProximityExistingLandUse(inputgeodfsql, com_parcel_data, 1995, lu)
