from LUCIS_AnalysisFunctions import PhysicalNoiseQuality
from settings import noise_barrier_def


def urbcomNoiseQuality(inputgeodfsql):
    lu = 'com'
    return PhysicalNoiseQuality(inputgeodfsql, noise_barrier_def, lu)
