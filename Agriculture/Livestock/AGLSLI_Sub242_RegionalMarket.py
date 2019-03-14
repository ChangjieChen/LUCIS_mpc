from LUCIS_AnalysisFunctions import AgricultureMarket


def aglsliRegionalMarket(inputgeodfsql):
    return AgricultureMarket(inputgeodfsql, 'livestock low intensity', 'regional')
