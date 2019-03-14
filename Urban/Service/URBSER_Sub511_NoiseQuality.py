from LUCIS_AnalysisFunctions import PhysicalNoiseQuality
from settings import noise_barrier_def


def urbserNoiseQuality(inputgeodfsql):
    lu = 'ser'
    return PhysicalNoiseQuality(inputgeodfsql, noise_barrier_def, lu)

