from LUCIS_AnalysisFunctions import AgricultureMarket


def agtmbrRegionalMarket(inputgeodfsql):
    return AgricultureMarket(inputgeodfsql, 'timber', 'regional')
