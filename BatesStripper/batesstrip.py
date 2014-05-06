"""
Blank a certain number of pixels on the bottom of the tiff to remove the bates stamp.
Requires GraphicsMagic commands be installed.
"""
from __future__ import print_function
import os
import subprocess
import re
from collections import defaultdict


def dirlist(root_dir, ffilter='*.*'):
    if not os.path.isdir(root_dir):
        raise OSError('Input directory %s does not exist' % root_dir)

    result = []
    root_dir = root_dir + os.sep + ffilter
    dirargs = ['dir', root_dir, '/B', '/S', '/A:-D']
    proc = subprocess.Popen(dirargs, shell=True, stdout=subprocess.PIPE, bufsize=1)
    for dirline in iter(proc.stdout.readline, b''):
        result.append(dirline.strip())
    proc.communicate()
    return result


def dimgrouping(files):
    if len(files) < 1:
        return {}
    gmargs = ['gm', 'identify']
    gmargs.extend(files)
    try:
        diminfo = subprocess.check_output(gmargs)
    except subprocess.CalledProcessError as e:
        e.message = 'Error in image identify operation\n%s' % diminfo
        raise e

    diminfo = [x for x in diminfo.split('\r\n') if x is not '']
    dimre = re.compile(r'\d+x\d+')
    results = defaultdict(list)
    for i, line in enumerate(diminfo):
        #print dimre.findall(line)
        dim = tuple(dimre.findall(line)[0].split('x'))
        results[dim].append(files[i])
    return results


def editimages(dim_imageset, pixels):
    for dim in dim_imageset.keys():
        w, h = int(dim[0]), int(dim[1])
        gmargs = ['gm', 'mogrify', '-fill', 'white', '-draw']
        gmargs.append('rectangle %s,%s %s,%s' % (0, h-pixels, w, h))
        gmargs.extend(dim_imageset[dim])
        try:
            subprocess.check_call(gmargs, shell=True)
        except subprocess.CalledProcessError as e:
            print('gm command failed!\ncmd', e.cmd)
        #print gmargs


def filechunks(files, chunksize):
    for i in xrange(0, len(files), chunksize):
        yield files[i:i+chunksize]

if __name__ == '__main__':
    import argparse
    import pprint as pp

    parser = argparse.ArgumentParser(description=
                                    """
                                    Whites out the bottom pixels of all tiffs in a directory to remove bates stamps.
                                    WARNING: Edits tiffs in place so make sure they are a copy.
                                    """)
    parser.add_argument('-directory', '-d', action='store',
                        help='directory of tiffs to edit')
    parser.add_argument('-pixels', '-p', action='store', type=int,
                        help='number of pixels from the bottom to clear')
    parser.add_argument('-b', action='store', type=int, default=50,
                        help='Batch size to feed gm at a time.  Usually just use the default.')

    args = parser.parse_args()

    print("\n Scanning directory...", end='\r')
    flist = dirlist(args.directory, ffilter='*.tif')
    totalfiles = len(flist)
    cnt = 0
    for chunk in filechunks(flist, args.b):
        #pp.pprint(dict(dimgrouping(chunk)))
        dimgroup = dimgrouping(chunk)
        editimages(dimgroup, args.pixels)
        cnt += args.b
        print(' ', cnt, 'of', totalfiles, 'processed', end='\r')
    print(' Complete.                        ')
