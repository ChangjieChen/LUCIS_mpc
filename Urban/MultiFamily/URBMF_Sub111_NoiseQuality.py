from LUCIS_AnalysisFunctions import PhysicalNoiseQuality
from settings import noise_barrier_def


def urbmfNoiseQuality(inputgeodfsql):
    lu = 'mf'
    return PhysicalNoiseQuality(inputgeodfsql, noise_barrier_def, lu)
