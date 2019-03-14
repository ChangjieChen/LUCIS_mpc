from LUCIS_AnalysisFunctions import AgricultureMarket


def aglshiLocalMarket(inputgeodfsql):
    return AgricultureMarket(inputgeodfsql, 'livestock high intensity', 'local')
