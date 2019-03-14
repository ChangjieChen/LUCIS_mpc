from LUCIS_AnalysisFunctions import ProximityServiceAmenities
from settings import ser_parcel_data


def urbserServiceAmenities(inputgeodfsql):
    lu = 'ser'
    service_wgt = "SELECT class, weight FROM wgt_urbmodel " \
                  "WHERE model = 'service' AND land_use_type = '{}'".format(lu)
    return ProximityServiceAmenities(inputgeodfsql, ser_parcel_data, "", service_wgt, lu)

