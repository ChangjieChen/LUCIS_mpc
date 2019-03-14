import time
import os
import importlib.util as implib
import multiprocessing as mp
from settings import vr_parcel_data
import pandas as pd

path = r'D:\OneDrive - University of Florida\Research\Dissertation\LUCIS_mpc\Urban\MultiFamily\\'
works = [item for item in os.listdir(path) if item.endswith('py')]
# objs = ['Obj' + _ for _ in list(set([script[script.find('Sub')+3:script.find('Sub')+5]
#                                      for script in scripts if 'Sub' in script]))]
# works = [script for script in scripts if not any(obj in script for obj in objs)]


def SubObjWorker(path, script, inputgeodfsql):
    mod_name = script.split('_')[0].lower() + script.split('_')[-1].split('.')[0]
    spec = implib.spec_from_file_location(mod_name, path + script)
    mod = implib.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return getattr(mod, mod_name)(inputgeodfsql)


def ParallelSubObj(args):
    return SubObjWorker(*args)


# def ObjWorker(subobjoutput, path, script, inputgeodfsql):
#     mod_name = script.split('_')[0].lower() + script.split('_')[-1].split('.')[0]
#     spec = implib.spec_from_file_location(mod_name, path + script)
#     mod = implib.module_from_spec(spec)
#     spec.loader.exec_module(mod)
#     return getattr(mod, mod_name)(subobjoutput, inputgeodfsql)
# def ParallelObj(args):
#     return ObjWorker(*args)


if __name__ == '__main__':
    t1 = time.time()
    print("Program started at {}".format(time.ctime()))
    subpool = mp.Pool()
    subpool_output = subpool.map(ParallelSubObj, [[path, subobj, vr_parcel_data] for subobj in works])
    subpool.close()
    subpool.join()
    print("Sub-objectives finished at {}, Objectives started.".format(time.ctime()))
    # objpool = mp.Pool(1)
    # objpool.map(ParallelObj, [[subpool_output, path, obj, vr_parcel_data] for obj in objs])
    # print("Objectives finished at {}".format(time.ctime()))
    # t2 = time.time()
    # print("Total time taken {} minutes".format(str(round((t2-t1)/60,2))))
    final = pd.concat(subpool_output, axis=1, keys=[_.name for _ in subpool_output])
    finaloutput = r"D:\OneDrive - University of Florida\Research\Dissertation\mpc output\mfoutput.csv"
    final.to_csv(finaloutput, sep=',')
    print(final)
