from LUCIS_AnalysisFunctions import ProximityLandValue
from settings import ind_parcel_data


def urbindLandValue(inputgeodfsql):
    lu = 'ind'
    return ProximityLandValue(inputgeodfsql, ind_parcel_data, lu)
