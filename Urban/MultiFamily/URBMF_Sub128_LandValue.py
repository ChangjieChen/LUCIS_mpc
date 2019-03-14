from LUCIS_AnalysisFunctions import ProximityLandValue
from settings import mf_parcel_data


def urbmfLandValue(inputgeodfsql):
    lu = 'mf'
    return ProximityLandValue(inputgeodfsql, mf_parcel_data, lu)
