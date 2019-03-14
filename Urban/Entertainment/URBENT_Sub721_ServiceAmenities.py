from LUCIS_AnalysisFunctions import ProximityServiceAmenities
from settings import ent_parcel_data


def urbentServiceAmenities(inputgeodfsql):
    lu = 'ent'
    service_wgt = "SELECT class, weight FROM wgt_urbmodel " \
                  "WHERE model = 'service' AND land_use_type = '{}'".format(lu)
    return ProximityServiceAmenities(inputgeodfsql, ent_parcel_data, '', service_wgt, lu)
