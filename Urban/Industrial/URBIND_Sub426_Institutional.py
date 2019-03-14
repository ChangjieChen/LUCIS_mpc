from LUCIS_AnalysisFunctions import ProximityInverse
from settings import ind_parcel_data, ins_parcel_data


def urbindInstitutional(inputgeodfsql):
    lu = 'ind'
    return ProximityInverse(inputgeodfsql, ind_parcel_data, ins_parcel_data, 'institutional', lu)

