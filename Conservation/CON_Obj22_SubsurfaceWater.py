from LUCIS_AnalysisFunctions import ClipModelRescale


def conSubsurfaceWater(inputgeodfsql):
    return ClipModelRescale(inputgeodfsql, 'subsurface water')
