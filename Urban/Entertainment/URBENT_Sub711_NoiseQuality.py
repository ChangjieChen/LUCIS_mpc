from LUCIS_AnalysisFunctions import PhysicalNoiseQuality
from settings import noise_barrier_def


def urbentNoiseQuality(inputgeodfsql):
    lu = 'ent'
    return PhysicalNoiseQuality(inputgeodfsql, noise_barrier_def, lu)
