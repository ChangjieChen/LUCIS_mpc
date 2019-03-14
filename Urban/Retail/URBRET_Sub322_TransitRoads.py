from LUCIS_AnalysisFunctions import ProximityTransitRoads
from settings import ret_parcel_data, cell_size


def urbretTransitRoads(inputgeodfsql):
    lu = 'ret'
    transitroads_wgt = "SELECT class, weight FROM wgt_urbmodel " \
                       "WHERE model = 'transitroads' AND land_use_type = '{}'".format(lu)
    return ProximityTransitRoads(inputgeodfsql, ret_parcel_data, cell_size, transitroads_wgt, lu)

