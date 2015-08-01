#!/usr/env python
# NB: if you write to local HDFS, ensure that the directories
# "/user/ubuntu/CSFROcsv/vals" and "/user/ubuntu/CSFROcsv/mask" exist
#
# This code converts a GRIB climate dataset stored on S3 to csv and either writes it to local HDFS or back to S3.
# It can be run simultaneously in several processes on several machines, to facilitate processing large amounts of data,
# and can be stopped and restarted (assuming it isn't stopped while in the process of actually writing a file) without
# reprocessing or missing processing of any of the GRIB files.
#
# call with convertGRIBtoCSV.py AWS_KEY AWS_SECRET_KEY NUMPROCESSES MYREMAINDER (see below for explanation)

from boto.s3.connection import S3Connection
from boto.s3.key import Key
import numpy as np
import pydoop.hdfs as hdfs
import pygrib
import gzip
from datetime import datetime
import os, sys

# output_to_S3_flag: false, if want output to be stored on local HDFS
# compressed_flag:  if true, output will be gzip compressed
# numprocesses needs to be prime, to ensure only one process tries to process each record
# myremainder should be a unique number between 0 and process - 1 that identifies the records this process will attempt to convert

def convertGRIBs(aws_key, aws_secret_key, numprocesses, myremainder, compressed_flag = False, output_to_S3_flag = False):

    conn = S3Connection(aws_key, aws_secret_key)

    # source of the CSFR-O data, as a set of grb2 files
    bucket = conn.get_bucket('agittens')
    keys = bucket.list(prefix='CSFR-O/grib2/ocnh06.gdas.197901')

    # make the vals and masks vectors global because they're huge, so don't want to reallocate them 
    dimsperlevel = 360*720
    dims = dimsperlevel * 41
    vals = np.zeros((dims,))
    mask = np.zeros((dims,))
    runningmask = np.zeros((dims,)) # so we can track how much of each entry is missing, over the entire dataset

    # returns the set of gribs from an s3 key 
    def get_grib_from_key(inkey):
        with open('temp{0}'.format(myremainder), 'w') as fin:
            inkey.get_file(fin)
        return pygrib.open('temp{0}'.format(myremainder))

    # index of the gribs within the grib file that correspond to SST and sub surface sea temperatures
    gribindices = range(1,41)
    gribindices.append(207)
    gribindices = list(reversed(gribindices))

    # for a given set of gribs, extracts the desired temperature observations and converts them to a vector of
    # observations and a mask vector indicating which observations are missing
    def converttovec(grbs):

        for index in range(41):
            maskedobs = grbs[gribindices[index]].data()[0]
            obs = maskedobs.data
            obs[maskedobs.mask] = 0
            vals[index*dimsperlevel:(index+1)*dimsperlevel] = obs.reshape((dimsperlevel,))
            mask[index*dimsperlevel:(index+1)*dimsperlevel] = maskedobs.mask.reshape((dimsperlevel,))
            
        return vals,mask

    # prints a given status message with a timestamp
    def report(status):
        print datetime.now().time().isoformat() + ":\t" + status

    # convenience function so can write to a compressed or uncompressed file transparently
    if compressed_flag:
        myopen = gzip.open
    else:
        myopen = open
        
    error_fh = open('grib_conversion_error_log_{0}_of_{1}'.format(myremainder, numprocesses), 'w')

    for (recordnum, inkey) in enumerate(keys):
        # only process records assigned to you
        if (recordnum % numprocesses) is not myremainder:
            continue
        
        # choose the right name for the vector of observations and the vector of masks
        # depending on whether or not they're compressed
        valsfname = "CSFROcsv/vals/part-"+format(recordnum,"05")
        maskfname = "CSFROcsv/mask/part-"+format(recordnum,"05")
        if compressed_flag:
            valsfname += ".gz"
            maskfname += ".gz"
            
        # avoid processing this set of observations if it has already been converted
        if output_to_S3_flag:
            possible_key = bucket.get_key(valsfname)
            if possible_key is not None:
                report("{0} already converted to csv, skipping record {1}".format(inkey.name, recordnum))
                continue
        else:
            if hdfs.path.isfile(valsfname):
                report("{0} already converted to csv, skipping record {1}".format(inkey.name, recordnum))
                continue
                
        # convert the observations and write them out to HDFS/S3 compressed/uncompressed
        try:
            grbs = get_grib_from_key(inkey)
            report("Retrieved {0} from S".format(inkey.name))
        
            (vals, mask) = converttovec(grbs)
            report("Converted {0} to numpy arrays".format(inkey.name))
        
            tempvalsfname = 'tempvals{0}'.format(myremainder)
            tempmaskfname = 'tempmask{0}'.format(myremainder)
            with myopen(tempvalsfname, 'w') as valsfout:
                with myopen(tempmaskfname, 'w') as maskfout:
                    for index in range(0, vals.shape[0]):
                        if (vals[index] > 0):
                            valsfout.write("{0},{1},{2}\n".format(recordnum, index, vals[index]))
                        if (mask[index] > 0):
                            maskfout.write("{0},{1},{2}\n".format(recordnum, index, mask[index]))
            report("Wrote numpy arrays to local files")
            
            if output_to_S3_flag:
                valsoutkey = Key(bucket)
                valsoutkey.key = valsfname
                valsoutkey.set_contents_from_filename(tempvalsfname)
                maskoutkey = Key(bucket)
                maskoutkey.key = maskfname
                maskoutkey.set_contents_from_filename(tempmaskfname)
                report("Wrote {0} to {1} and {2} on S3".format(inkey.name, valsfname, maskfname))
            else:
                hdfs.put(tempvalsfname, valsfname)
                hdfs.put(tempmaskfname, maskfname)
                report("Wrote {0} to {1} and {2} on HDFS".format(inkey.name, valsfname, maskfname))
	    os.remove(tempvalsfname)
            os.remove(tempmaskfname)
        except:
            report("Skipping record {0}! An error occurred processing {1}".format(recordnum, inkey.name))
            error_fh.write("Skipped {1}, record {0}\n".format(inkey.name, recordnum))
            
    error_fh.close()
    try:
        os.remove('temp{0}'.format(myremainder))
        os.remove(tempvalsfname)
        os.remove(tempmaskfname)
    except:
        pass

if __name__ == "__main__":
    convertGRIBs(sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4]))


