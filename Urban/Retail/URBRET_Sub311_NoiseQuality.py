from LUCIS_AnalysisFunctions import PhysicalNoiseQuality
from settings import noise_barrier_def


def urbretNoiseQuality(inputgeodfsql):
    lu = 'ret'
    return PhysicalNoiseQuality(inputgeodfsql, noise_barrier_def, lu)

