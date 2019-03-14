from LUCIS_AnalysisFunctions import AgricultureMarket


def aglshiRegionalMarket(inputgeodfsql):
    return AgricultureMarket(inputgeodfsql, 'livestock high intensity', 'regional')
