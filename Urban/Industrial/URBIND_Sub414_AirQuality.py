from LUCIS_AnalysisFunctions import PhysicalAirQuality
from settings import ret_parcel_data


def urbindAirQuality(inputgeodfsql):
    lu = 'ind'
    return PhysicalAirQuality(inputgeodfsql, ret_parcel_data, lu)
