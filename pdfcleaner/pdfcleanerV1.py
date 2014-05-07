#Flattens a pdf and removes OCR and bookmarks by round tripping to tiff and back
#Requirements:
#gs command from the Ghostscript package
#tiffcp and tiff2pdf from libtiff.net package
__version__ = 1.0
import subprocess
import os
import sys
import time
import tempfile
import hashlib
import errno
import shutil
import multiprocessing
import pprint as pp
from os.path import join as joinp


def gather_pdfs(targdir):
    result = []
    dirargs = ['dir', targdir, '/S', '/B', '/A-D']
    proc = subprocess.Popen(dirargs, shell=True, stdout=subprocess.PIPE, bufsize=1)
    for dirline in iter(proc.stdout.readline, b''):
        result.append(dirline.strip())
    proc.communicate()
    allowed_ext = ['.pdf', '.PDF', '.Pdf', '.pDf', '.pdF', '.PDf', '.pDF', '.PdF']
    result = [x for x in result if x[-4:] in allowed_ext]
    return result


def gen_fileinfo(filepath, temproot, sourceroot, resultroot):
    """
    Returns a dict with all the  state necessary to take the file through the whole process.
    """
    sourceroot = sourceroot.rstrip(os.sep) + os.sep
    resultroot = resultroot.rstrip(os.sep) + os.sep
    fileinfo = {
        'origfilename': os.path.basename(filepath),
        'origdir': os.path.dirname(filepath),
        'tempfilehash': hashlib.md5(filepath).hexdigest(),
        'errors': ''}
    fileinfo['workingdir'] = joinp(temproot, fileinfo['tempfilehash'])
    fileinfo['subpath'] = filepath.replace(sourceroot, '')
    fileinfo['resultpath'] = joinp(resultroot, fileinfo['subpath'])
    fileinfo['resultdir'] = os.path.dirname(fileinfo['resultpath'])
    return fileinfo


def create_dir(newdir):
    """
    Create directory if it doesn't already exist.
    """
    try:
        os.makedirs(newdir)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(newdir):
            pass
        else:
            raise


def copy_working_file(finfo):
    """
    Copy the initial file to the temp working dir renamed with a unique hash.
    Also create the other working directories.
    """
    create_dir(finfo['workingdir'])
    shutil.copyfile(joinp(finfo['origdir'], finfo['origfilename']),
                    joinp(finfo['workingdir'], finfo['tempfilehash']+'.pdf'))


def dummywork(finfo, lck):
    with lck:
        print 'Working...'
    #time.sleep(7)
    shutil.copyfile(joinp(finfo['workingdir'], finfo['tempfilehash']+'.tif'),
                    joinp(finfo['workingdir'], finfo['tempfilehash']+'.pdf.complete'))


def copy_result(finfo):
    """
    Copy the result of all the processing from the temp dir to the final out dir.
    Also clean up the temp directory for the file.
    """
    create_dir(finfo['resultdir'])
    try:
        shutil.copyfile(joinp(finfo['workingdir'], finfo['tempfilehash']+'.pdf.complete'),
                        finfo['resultpath'])
    except IOError as e:
        finfo['errors'] += 'File copy:\n' + e.filename + '\n'
    shutil.rmtree(finfo['workingdir'])


def burst_pdf(finfo):
    gsargs = ['gswin64c', '-SDEVICE=tiffg4', '-r300x300', '-o',
              joinp(finfo['workingdir'], finfo['tempfilehash'] + '.tif'),
              joinp(finfo['workingdir'], finfo['tempfilehash'] + '.pdf')]
    # try:
    #     out = subprocess.check_output(gsargs, stderr=subprocess.STDOUT, shell=True)
    # except subprocess.CalledProcessError as e:
    #     pass
    gsproc = subprocess.Popen(gsargs, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, errs = gsproc.communicate()
    if gsproc.returncode > 0:
        finfo['errors'] += 'Ghostscript:\n' + errs + '\n'


def merge_tiff(finfo):
    tiff2pdfargs = ['tiff2pdf', '-o', joinp(finfo['workingdir'], finfo['tempfilehash'] + '.pdf.complete'),
                    joinp(finfo['workingdir'], finfo['tempfilehash'] + '.tif')]
    #out = subprocess.check_output(tiff2pdfargs, shell=True)
    t2pproc = subprocess.Popen(tiff2pdfargs, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, errs = t2pproc.communicate()
    if out:
        finfo['errors'] += 'Tiff2Pdf:\n' + out + '\n'


class Worker(multiprocessing.Process):
    def __init__(self, workq, runinfo=None, errs=None):
        super(Worker, self).__init__()
        self.workq = workq
        self.runinfo = runinfo
        self.errs = errs

    def run(self):
        while True:
            workinfo = self.workq.get()
            if workinfo is None:
                if self.runinfo['verbose']:
                    with self.runinfo['stdout_lock']:
                        print('%s: Exiting' % self.name)
                self.workq.task_done()
                break

            if self.runinfo['verbose']:
                with self.runinfo['stdout_lock']:
                    print('%s: processing %s -- %s' % (self.name, workinfo['subpath'], self.workq.qsize()))
            copy_working_file(workinfo)
            burst_pdf(workinfo)
            merge_tiff(workinfo)
            copy_result(workinfo)
            if workinfo['errors']:
                self.errs.put(workinfo)
            if self.runinfo['verbose']:
                with self.runinfo['stdout_lock']:
                    print('%s: finished %s' % (self.name, workinfo['subpath']))
            self.workq.task_done()


if __name__ == '__main__':
    indir = sys.argv[1]
    outdir = sys.argv[2]
    temproot = tempfile.mkdtemp(prefix='PDFCLEAN-')
    files = gather_pdfs(indir)
    finfos = [gen_fileinfo(filepath, temproot, indir, outdir) for filepath in files]

    numworkers = 4
    verbose = False
    worker_runinfo = {'stdout_lock': multiprocessing.Lock(), 'verbose': verbose}
    workq = multiprocessing.JoinableQueue()
    errq = multiprocessing.Queue()
    workers = [Worker(workq, worker_runinfo, errq) for i in xrange(numworkers)]
    for worker in workers:
        worker.start()

    for fileinfo in finfos:
        workq.put(fileinfo)
    for i in xrange(numworkers):  # Add a poison pill for each consumer
        workq.put(None)

    workq.join()

    while errq.qsize() > 0:
        errfile = errq.get()
        print '\n!Errors for %s\n' % errfile['subpath']
        print errfile['errors']
        print '\n--------------------------------------------\n'

    # for info in finfo:
    #     copy_working_file(info)
    #     dummywork(info)
    #     copy_result(info)
    # #pp.pprint(finfo)

    shutil.rmtree(temproot)
