from LUCIS_AnalysisFunctions import ProximityTransitRoads
from settings import sf_parcel_data, cell_size


def urbsfTransitRoads(inputgeodfsql):
    lu = 'sf'
    transitroads_wgt = "SELECT class, weight FROM wgt_urbmodel " \
                       "WHERE model = 'transitroads' AND land_use_type = '{}'".format(lu)
    return ProximityTransitRoads(inputgeodfsql, sf_parcel_data, cell_size, transitroads_wgt, lu)

