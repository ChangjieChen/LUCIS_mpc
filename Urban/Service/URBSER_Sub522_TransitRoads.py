from LUCIS_AnalysisFunctions import ProximityTransitRoads
from settings import ser_parcel_data, cell_size


def urbserTransitRoads(inputgeodfsql):
    lu = 'ser'
    transitroads_wgt = "SELECT class, weight FROM wgt_urbmodel " \
                       "WHERE model = 'transitroads' AND land_use_type = '{}'".format(lu)
    return ProximityTransitRoads(inputgeodfsql, ser_parcel_data, cell_size, transitroads_wgt, lu)
