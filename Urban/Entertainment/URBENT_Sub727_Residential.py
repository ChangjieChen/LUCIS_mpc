from LUCIS_AnalysisFunctions import ProximityResidential
from settings import ent_parcel_data


def urbentResidential(inputgeodfsql):
    lu = 'ent'
    return ProximityResidential(inputgeodfsql, ent_parcel_data, lu)

