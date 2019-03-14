from LUCIS_AnalysisFunctions import AgricultureMarket


def aglsliLocalMarket(inputgeodfsql):
    return AgricultureMarket(inputgeodfsql, 'livestock low intensity', 'local')
