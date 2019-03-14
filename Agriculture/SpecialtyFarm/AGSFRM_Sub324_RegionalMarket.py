from LUCIS_AnalysisFunctions import AgricultureMarket


def agsfrmRegionalMarket(inputgeodfsql):
    return AgricultureMarket(inputgeodfsql, 'specialty farm', 'regional')
