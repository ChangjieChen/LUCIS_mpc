from LUCIS_AnalysisFunctions import PhysicalAirQuality
from settings import sf_parcel_data


def urbsfAirQuality(inputgeodfsql):
    lu = 'sf'
    return PhysicalAirQuality(inputgeodfsql, sf_parcel_data, lu)

