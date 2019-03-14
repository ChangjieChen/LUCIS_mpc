from LUCIS_AnalysisFunctions import PhysicalAirQuality
from settings import ser_parcel_data


def urbserAirQuality(inputgeodfsql):
    lu = 'ser'
    return PhysicalAirQuality(inputgeodfsql, ser_parcel_data, lu)

