import umsgpack
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.image as mpimg

numlevels = 40
numlats = 360
numlons = 720

def restore_EOF(vec, observedLocations, fillValue = 0):
    eof = np.empty((numlevels, numlats, numlons))
    curLoc = 0
    observedCounter = 0
    for levnum in xrange(numlevels):
        for latnum in xrange(numlats):
            for lonnum in xrange(numlons):
                if (curLoc == observedLocations[observedCounter]):
                    eof[levnum, latnum, lonnum] = vec[observedCounter]
                    observedCounter = observedCounter + 1
                else:
                    eof[levnum, latnum, lonnum] = fillValue
                curLoc = curLoc + 1
                if (observedCounter == len(vec)):
                    return eof

def normalize(arr):
    ''' Function to normalize an input array to 0-1 '''
    arr_min = arr.min()
    arr_max = arr.max()
    return (arr - arr_min) / (arr_max - arr_min)

eofs_fname = "../conversion-code/ocean_conversion/testOutputs/testeofs.nc.msgpack"
f = open(eofs_fname, 'rb')
(numeofs, numobs, numgridpts, U, V, S, mean) = umsgpack.load(f)

metadata = np.load("../conversion-code/ocean_conversion/testOutputs/oceanMetadata.npz")

meanTempEOF = restore_EOF(normalize(np.array(mean)), metadata["observedLocations"])
plt.imshow(meanTempEOF[numlevels-1,...])
