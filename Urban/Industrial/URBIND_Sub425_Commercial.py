from LUCIS_AnalysisFunctions import ProximityInverse
from settings import ind_parcel_data, com_parcel_data


def urbindCommercial(inputgeodfsql):
    lu = 'ind'
    return ProximityInverse(inputgeodfsql, ind_parcel_data, com_parcel_data, 'commercial', lu)
