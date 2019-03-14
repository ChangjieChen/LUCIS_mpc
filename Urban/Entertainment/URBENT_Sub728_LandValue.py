from LUCIS_AnalysisFunctions import ProximityLandValue
from settings import ent_parcel_data


def urbentLandValue(inputgeodfsql):
    lu = 'ent'
    return ProximityLandValue(inputgeodfsql, ent_parcel_data, lu)
