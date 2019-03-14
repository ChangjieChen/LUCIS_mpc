from LUCIS_AnalysisFunctions import ProximityInterstate
from settings import ind_parcel_data, cell_size


def urbindInterstate(inputgeodfsql):
    lu = 'ind'
    return ProximityInterstate(inputgeodfsql, ind_parcel_data, cell_size, lu)
