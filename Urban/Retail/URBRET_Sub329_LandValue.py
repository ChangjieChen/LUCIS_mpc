from LUCIS_AnalysisFunctions import ProximityLandValue
from settings import ret_parcel_data


def urbretLandValue(inputgeodfsql):
    lu = 'ret'
    return ProximityLandValue(inputgeodfsql, ret_parcel_data, lu)
