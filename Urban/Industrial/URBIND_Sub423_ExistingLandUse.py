from LUCIS_AnalysisFunctions import ProximityExistingLandUse
from settings import ind_parcel_data


def urbindExistingLandUse(inputgeodfsql):
    lu = 'ind'
    return ProximityExistingLandUse(inputgeodfsql, ind_parcel_data, 1995, lu)
