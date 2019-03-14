from LUCIS_AnalysisFunctions import ProximityLandValue
from settings import ins_parcel_data


def urbinsLandValue(inputgeodfsql):
    lu = 'ins'
    return ProximityLandValue(inputgeodfsql, ins_parcel_data, lu)
