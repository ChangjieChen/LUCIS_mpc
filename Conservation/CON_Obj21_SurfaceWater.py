from LUCIS_AnalysisFunctions import ClipModelRescale


def conSurfaceWater(inputgeodfsql):
    return ClipModelRescale(inputgeodfsql, 'surface water')
