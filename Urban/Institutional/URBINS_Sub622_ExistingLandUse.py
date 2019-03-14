from LUCIS_AnalysisFunctions import ProximityExistingLandUse
from settings import ins_parcel_data


def urbinsExistingLandUse(inputgeodfsql):
    lu = 'ins'
    return ProximityExistingLandUse(inputgeodfsql, ins_parcel_data, 1995, lu)
