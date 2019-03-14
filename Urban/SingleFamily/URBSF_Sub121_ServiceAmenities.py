from LUCIS_AnalysisFunctions import ProximityServiceAmenities
from settings import sf_parcel_data


def urbsfServiceAmenities(inputgeodfsql):
    lu = 'sf'
    school_wgt = "SELECT class, weight FROM wgt_urbmodel " \
                 "WHERE model = 'schoolsystem' AND land_use_type = '{}'".format(lu)
    service_wgt = "SELECT class, weight FROM wgt_urbmodel " \
                  "WHERE model = 'service' AND land_use_type = '{}'".format(lu)
    return ProximityServiceAmenities(inputgeodfsql, sf_parcel_data, school_wgt, service_wgt, lu)
