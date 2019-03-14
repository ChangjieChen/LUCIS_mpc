from LUCIS_AnalysisFunctions import AgricultureMarket


def agtmbrLocalMarket(inputgeodfsql):
    return AgricultureMarket(inputgeodfsql, 'timber', 'local')
