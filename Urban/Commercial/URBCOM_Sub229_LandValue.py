from LUCIS_AnalysisFunctions import ProximityLandValue
from settings import com_parcel_data


def urbcomLandValue(inputgeodfsql):
    lu = 'com'
    return ProximityLandValue(inputgeodfsql, com_parcel_data, lu)
