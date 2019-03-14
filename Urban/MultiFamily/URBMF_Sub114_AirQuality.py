from LUCIS_AnalysisFunctions import PhysicalAirQuality
from settings import mf_parcel_data


def urbmfAirQuality(inputgeodfsql):
    lu = 'mf'
    return PhysicalAirQuality(inputgeodfsql, mf_parcel_data, lu)
