from LUCIS_AnalysisFunctions import ProximityServiceAmenities
from settings import ret_parcel_data


def urbretServiceAmenities(inputgeodfsql):
    lu = 'ret'
    service_wgt = "SELECT class, weight FROM wgt_urbmodel " \
                  "WHERE model = 'service' AND land_use_type = '{}'".format(lu)
    return ProximityServiceAmenities(inputgeodfsql, ret_parcel_data, "", service_wgt, lu)

