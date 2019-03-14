from LUCIS_AnalysisFunctions import PhysicalAirQuality
from settings import ins_parcel_data


def urbinsAirQuality(inputgeodfsql):
    lu = 'ins'
    return PhysicalAirQuality(inputgeodfsql, ins_parcel_data, lu)
