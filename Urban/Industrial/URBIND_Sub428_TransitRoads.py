from LUCIS_AnalysisFunctions import ProximityTransitRoads
from settings import ind_parcel_data, cell_size


def urbindTransitRoads(inputgeodfsql):
    lu = 'ind'
    transitroads_wgt = "SELECT class, weight FROM wgt_urbmodel " \
                       "WHERE model = 'transitroads' AND land_use_type = '{}'".format(lu)
    return ProximityTransitRoads(inputgeodfsql, ind_parcel_data, cell_size, transitroads_wgt, lu)

