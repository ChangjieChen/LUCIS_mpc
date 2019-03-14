from LUCIS_AnalysisFunctions import ProximityLandValue
from settings import ser_parcel_data


def urbserLandValue(inputgeodfsql):
    lu = 'ser'
    return ProximityLandValue(inputgeodfsql, ser_parcel_data, lu)
