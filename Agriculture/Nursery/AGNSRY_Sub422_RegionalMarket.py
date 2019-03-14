from LUCIS_AnalysisFunctions import AgricultureMarket


def agnsryRegionalMarket(inputgeodfsql):
    return AgricultureMarket(inputgeodfsql, 'nursery', 'regional')
