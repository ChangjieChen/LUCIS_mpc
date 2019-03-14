from LUCIS_AnalysisFunctions import ProximityServiceAmenities
from settings import com_parcel_data


def urbcomServiceAmenities(inputgeodfsql):
    lu = 'com'
    service_wgt = "SELECT class, weight FROM wgt_urbmodel " \
                  "WHERE model = 'service' AND land_use_type = '{}'".format(lu)
    return ProximityServiceAmenities(inputgeodfsql, com_parcel_data, '', service_wgt, lu)

