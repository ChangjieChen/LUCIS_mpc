from LUCIS_AnalysisFunctions import ProximityTransitRoads
from settings import com_parcel_data, cell_size


def urbcomTransitRoads(inputgeodfsql):
    lu = 'com'
    transitroads_wgt = "SELECT class, weight FROM wgt_urbmodel " \
                       "WHERE model = 'transitroads' AND land_use_type = '{}'".format(lu)
    return ProximityTransitRoads(inputgeodfsql, com_parcel_data, cell_size, transitroads_wgt, lu)
