from LUCIS_AnalysisFunctions import ProximityExistingLandUse
from settings import mf_parcel_data


def urbmfExistingLandUse(inputgeodfsql):
    lu = 'mf'
    return ProximityExistingLandUse(inputgeodfsql, mf_parcel_data, 2000, lu)
