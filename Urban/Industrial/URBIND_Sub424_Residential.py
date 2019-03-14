from LUCIS_AnalysisFunctions import ProximityInverse
from settings import ind_parcel_data, res_parcel_data


def urbindResidential(inputgeodfsql):
    lu = 'ind'
    return ProximityInverse(inputgeodfsql, ind_parcel_data, res_parcel_data, 'residential', lu)
