from LUCIS_AnalysisFunctions import ProximityInverse
from settings import ind_parcel_data, ret_parcel_data


def urbindRetail(inputgeodfsql):
    lu = 'ind'
    return ProximityInverse(inputgeodfsql, ind_parcel_data, ret_parcel_data, 'retail', lu)


