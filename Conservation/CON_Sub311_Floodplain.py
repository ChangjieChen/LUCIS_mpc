from LUCIS_AnalysisFunctions import ClipModelRescale


def conFloodplain(inputgeodfsql):
    return ClipModelRescale(inputgeodfsql, 'floodplain')
