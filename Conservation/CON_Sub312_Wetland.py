from LUCIS_AnalysisFunctions import ClipModelRescale


def conWetland(inputgeodfsql):
    return ClipModelRescale(inputgeodfsql, 'wetland')
