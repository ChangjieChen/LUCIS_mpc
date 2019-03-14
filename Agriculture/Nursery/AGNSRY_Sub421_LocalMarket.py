from LUCIS_AnalysisFunctions import AgricultureMarket


def agnsryLocalMarket(inputgeodfsql):
    return AgricultureMarket(inputgeodfsql, 'nursery', 'local')
