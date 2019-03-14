from LUCIS_AnalysisFunctions import PhysicalNoiseQuality
from settings import noise_barrier_def


def urbindNoiseQuality(inputgeodfsql):
    lu = 'ind'
    return PhysicalNoiseQuality(inputgeodfsql, noise_barrier_def, lu)
