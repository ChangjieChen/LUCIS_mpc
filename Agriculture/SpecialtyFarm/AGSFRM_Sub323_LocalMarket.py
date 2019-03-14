from LUCIS_AnalysisFunctions import AgricultureMarket


def agsfrmLocalMarket(inputgeodfsql):
    return AgricultureMarket(inputgeodfsql, 'specialty farm', 'local')
