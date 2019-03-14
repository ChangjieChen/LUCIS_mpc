from LUCIS_AnalysisFunctions import ProximityInverse
from settings import sf_parcel_data, prison_data


def urbsfPrison(inputgeodfsql):
    lu = 'sf'
    return ProximityInverse(inputgeodfsql, sf_parcel_data, prison_data, 'prison', lu)
