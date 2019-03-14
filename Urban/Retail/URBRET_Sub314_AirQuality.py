from LUCIS_AnalysisFunctions import PhysicalAirQuality
from settings import ret_parcel_data


def urbretAirQuality(inputgeodfsql):
    lu = 'ret'
    return PhysicalAirQuality(inputgeodfsql, ret_parcel_data, lu)