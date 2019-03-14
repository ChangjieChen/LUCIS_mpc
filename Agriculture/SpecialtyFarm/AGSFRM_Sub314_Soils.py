from LUCIS_AnalysisFunctions import AgricultureSoil


def agsfrmSoils(inputgeodfsql):
    return AgricultureSoil(inputgeodfsql, 'specialty farm')
