from LUCIS_AnalysisFunctions import ProximityExistingLandUse
from settings import ent_parcel_data


def urbentExistingLandUse(inputgeodfsql):
    lu = 'ent'
    return ProximityExistingLandUse(inputgeodfsql, ent_parcel_data, 1995, lu)

