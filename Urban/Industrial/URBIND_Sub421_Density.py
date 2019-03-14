from LUCIS_AnalysisFunctions import ProximityDensity
from settings import cell_size, ind_parcel_data


def urbindDensity(inputgeodfsql):
    lu = 'ind'
    return ProximityDensity(inputgeodfsql, ind_parcel_data, cell_size, lu)

