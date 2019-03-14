from LUCIS_AnalysisFunctions import ProximityShopping
from settings import mf_parcel_data


def urbmfShopping(inputgeodfsql):
    lu = 'mf'
    return ProximityShopping(inputgeodfsql, mf_parcel_data, lu)
