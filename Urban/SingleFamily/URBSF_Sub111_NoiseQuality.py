from LUCIS_AnalysisFunctions import PhysicalNoiseQuality
from settings import noise_barrier_def


def urbsfNoiseQuality(inputgeodfsql):
    lu = 'sf'
    return PhysicalNoiseQuality(inputgeodfsql, noise_barrier_def, lu)
