from LUCIS_AnalysisFunctions import ProximityLandValue
from settings import sf_parcel_data


def urbsfLandValue(inputgeodfsql):
    lu = 'sf'
    return ProximityLandValue(inputgeodfsql, sf_parcel_data, lu)
