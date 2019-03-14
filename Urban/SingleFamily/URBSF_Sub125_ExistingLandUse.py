from LUCIS_AnalysisFunctions import ProximityExistingLandUse
from settings import sf_parcel_data


def urbsfExistingLandUse(inputgeodfsql):
    lu = 'sf'
    return ProximityExistingLandUse(inputgeodfsql, sf_parcel_data, 2000, lu)
