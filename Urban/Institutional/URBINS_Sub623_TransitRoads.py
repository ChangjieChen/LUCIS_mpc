from LUCIS_AnalysisFunctions import ProximityTransitRoads
from settings import ins_parcel_data, cell_size


def urbinsTransitRoads(inputgeodfsql):
    lu = 'ins'
    transitroads_wgt = "SELECT class, weight FROM wgt_urbmodel " \
                       "WHERE model = 'transitroads' AND land_use_type = '{}'".format(lu)
    return ProximityTransitRoads(inputgeodfsql, ins_parcel_data, cell_size, transitroads_wgt, lu)
