from LUCIS_AnalysisFunctions import PhysicalAirQuality
from settings import com_parcel_data


def urbcomAirQuality(inputgeodfsql):
    lu = 'com'
    return PhysicalAirQuality(inputgeodfsql, com_parcel_data, lu)
