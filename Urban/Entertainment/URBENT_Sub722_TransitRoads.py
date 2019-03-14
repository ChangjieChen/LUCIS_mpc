from LUCIS_AnalysisFunctions import ProximityTransitRoads
from settings import ent_parcel_data, cell_size


def urbentTransitRoads(inputgeodfsql):
    lu = 'ent'
    transitroads_wgt = "SELECT class, weight FROM wgt_urbmodel " \
                       "WHERE model = 'transitroads' AND land_use_type = '{}'".format(lu)
    return ProximityTransitRoads(inputgeodfsql, ent_parcel_data, cell_size, transitroads_wgt, lu)
