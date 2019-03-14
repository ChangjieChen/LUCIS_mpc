from LUCIS_AnalysisFunctions import ProximityTransitRoads
from settings import mf_parcel_data, cell_size


def urbmfTransitRoads(inputgeodfsql):
    lu = 'mf'
    transitroads_wgt = "SELECT class, weight FROM wgt_urbmodel " \
                       "WHERE model = 'transitroads' AND land_use_type = '{}'".format(lu)
    return ProximityTransitRoads(inputgeodfsql, mf_parcel_data, cell_size, transitroads_wgt, lu)
