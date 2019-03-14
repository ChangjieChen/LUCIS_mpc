from LUCIS_AnalysisFunctions import ProximityResidential
from settings import com_parcel_data


def urbcomResidential(inputgeodfsql):
    lu = 'com'
    return ProximityResidential(inputgeodfsql, com_parcel_data, lu)
