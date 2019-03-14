from LUCIS_AnalysisFunctions import ProximityInverse
from settings import mf_parcel_data, prison_data


def urbmfPrison(inputgeodfsql):
    lu = 'mf'
    return ProximityInverse(inputgeodfsql, mf_parcel_data, prison_data, 'prison', lu)
