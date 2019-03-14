from LUCIS_AnalysisFunctions import ProximityResidential
from settings import ser_parcel_data


def urbserResidential(inputgeodfsql):
    lu = 'ser'
    return ProximityResidential(inputgeodfsql, ser_parcel_data, lu)
