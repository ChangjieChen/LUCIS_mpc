from LUCIS_AnalysisFunctions import ProximityShopping
from settings import sf_parcel_data


def urbsfShopping(inputgeodfsql):
    lu = 'sf'
    return ProximityShopping(inputgeodfsql, sf_parcel_data, lu)
