from LUCIS_AnalysisFunctions import ProximityServiceAmenities
from settings import mf_parcel_data


def urbmfServiceAmenities(inputgeodfsql):
    lu = 'mf'
    school_wgt = "SELECT class, weight FROM wgt_urbmodel " \
                 "WHERE model = 'schoolsystem' AND land_use_type = '{}'".format(lu)
    service_wgt = "SELECT class, weight FROM wgt_urbmodel " \
                  "WHERE model = 'service' AND land_use_type = '{}'".format(lu)
    return ProximityServiceAmenities(inputgeodfsql, mf_parcel_data, school_wgt, service_wgt, lu)
