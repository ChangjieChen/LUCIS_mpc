from LUCIS_AnalysisFunctions import LandUseGrowth
from settings import sf_parcel_data


def urbsfGrowth(inputgeodfsql):
    lu = 'sf'
    growthrange = {'g8090': [1980, 1990, 0.8], 'g9000': [1990, 2000, 0.15], 'g0010': [2000, 2010, 0.05]}
    return LandUseGrowth(inputgeodfsql, sf_parcel_data, growthrange, lu)
