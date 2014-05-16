#Flattens a pdf and removes OCR and bookmarks by round tripping to tiff and back
#Requirements:
#gs command from the Ghostscript package
#tiffcp and tiff2pdf from libtiff.net package
#pdfinfo command from libpoppler.  Available on Windows in Cygwin
from __future__ import print_function
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
import Queue
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
        'pgcount': 0,
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


def pdf_info(finfo):
    pdfinfoargs = ['pdfinfo', joinp(finfo['origdir'], finfo['origfilename'])]
    pdfproc = subprocess.Popen(pdfinfoargs, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, errs = pdfproc.communicate()
    if pdfproc.returncode > 0:
        finfo['errors'] += 'PDFInfo:\n' + errs + '\n'
        return finfo
    outsplit = out.split()
    finfo['pgcount'] += int(outsplit[outsplit.index('Pages:') + 1])
    return finfo


def pagecount_parallel(finfos, proc_num=None):
    pool = multiprocessing.Pool(processes=proc_num)
    result = pool.map(pdf_info, finfos)
    pool.close()
    totalpages = sum([x['pgcount'] for x in result])
    errcount = len([x for x in result if x['errors']])
    return result, totalpages, errcount


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
        print('Working...')
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
    def __init__(self, workq, completeq, runinfo=None, errs=None):
        super(Worker, self).__init__()
        self.workq = workq
        self.completeq = completeq
        self.runinfo = runinfo
        self.errs = errs

    def run(self):
        while True:
            workinfo = self.workq.get()
            if workinfo is None:
                if self.runinfo['verbose']:
                    with self.runinfo['stdout_lock']:
                        print('%s: Exiting' % self.name)
                self.completeq.put(workinfo)
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
            else:
                self.completeq.put(workinfo)
            if self.runinfo['verbose']:
                with self.runinfo['stdout_lock']:
                    print('%s: finished %s' % (self.name, workinfo['subpath']))
            #with self.runinfo['mon_lock']:
            # self.runinfo['progmon'].logprimary_completion(workinfo['pgcount'])
            # self.runinfo['progmon'].logsecondary_completion(1)
            # print('worker:', id(self.runinfo['progmon']))
            self.workq.task_done()


class ProgressMonitor(object):
    def __init__(self, primarymetric_total=0, secondarymetric_total=0, window=0, minsamples=5):
        self.primarymetric_total = primarymetric_total
        self.secondarymetric_total = secondarymetric_total
        self.primarymetric_current = 0
        self.secondarymetric_current = 0
        self.primarymetric_history = []
        self.secondarymetric_history = []
        self.window = window
        self.minsamples = minsamples

    def startclock(self):
        self.starttime = time.time()

    def elapsed(self):
        return self._formattime(time.time() - self.starttime)

    def _formattime(self, secs):
        minu, sec = divmod(secs, 60)
        hour, minu = divmod(minu, 60)
        return '%d:%d:%.1f' % (hour, minu, sec)

    def logprimary_completion(self, units):
        self.primarymetric_current += units
        self.primarymetric_history.append((time.time(), units, self.primarymetric_current))

    def logsecondary_completion(self, units):
        self.secondarymetric_current += units
        self.secondarymetric_history.append((time.time(), units, self.secondarymetric_current))

    def _unitspersec(self, hist, minsamples, window=0):
        window *= -1
        workingset = hist[window:]
        if len(workingset) > minsamples:
            return (workingset[-1][2] - workingset[0][2]) / (workingset[-1][0] - workingset[0][0])
        else:
            return 0.0

    def remaining_byprimary(self):
        ups = self._unitspersec(self.primarymetric_history, self.minsamples, self.window)
        try:
            secremain = (self.primarymetric_total - self.primarymetric_current) / ups
        except ZeroDivisionError:
            return '--:--:--'
        return self._formattime(secremain)

    def remaining_bysecondary(self):
        ups = self._unitspersec(self.secondarymetric_history, self.minsamples, self.window)
        try:
            secremain = (self.secondarymetric_total - self.secondarymetric_current) / ups
        except ZeroDivisionError:
            return '--:--:--'
        return self._formattime(secremain)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description=
                                    """
                                    Utility that cleans all information from a pdf by converting it to black and
                                    white tiff and then back to pdf.
                                    """)
    parser.add_argument('-source', '-s', action='store',
                        help='Source directory of pdfs')
    parser.add_argument('-dest', '-d', action='store',
                        help='output directory of pdfs')
    parser.add_argument('-verbose', '-v', action='store_true',
                        help='Show verbose, per worker information')
    parser.add_argument('-processes', '-p', type=int, action='store', default=multiprocessing.cpu_count(),
                        help='Number of parallel processes, defaults to core count')
    args = parser.parse_args()
    progmon = ProgressMonitor()
    progmon.startclock()

    temproot = tempfile.mkdtemp(prefix='PDFCLEAN-')

    print('\n Pre-scanning PDFs...', end='\r')
    files = gather_pdfs(args.source)
    finfos = [gen_fileinfo(filepath, temproot, args.source, args.dest) for filepath in files]
    finfos, totpages, errcnt = pagecount_parallel(finfos)

    worker_runinfo = {'stdout_lock': multiprocessing.Lock(),
                      'verbose': args.verbose}
    workq = multiprocessing.JoinableQueue()
    errq = multiprocessing.Queue()
    completeq = multiprocessing.Queue()
    workers = [Worker(workq, completeq, worker_runinfo, errq) for i in xrange(args.processes)]
    for worker in workers:
        worker.start()

    progmon.logprimary_completion(0)
    progmon.logsecondary_completion(0)
    progmon.primarymetric_total = totpages
    progmon.secondarymetric_total = len(finfos)

    for fileinfo in finfos:
        workq.put(fileinfo)
    for i in xrange(args.processes):  # Add a poison pill for each consumer
        workq.put(None)

    nonecnt = 0
    while True:
        print(' '*70, end='\r')
        print(' %s/%s doc, %s/%s pages, compelete :: %s elapsed - %s remaining' % (
                                                        progmon.secondarymetric_current,
                                                        progmon.secondarymetric_total,
                                                        progmon.primarymetric_current,
                                                        progmon.primarymetric_total,
                                                        progmon.elapsed(),
                                                        progmon.remaining_byprimary()), end='\r')
        try:
            comp = completeq.get(block=False)
        except Queue.Empty:
            pass
        else:
            if comp is None:
                nonecnt += 1
            else:
                progmon.logprimary_completion(comp['pgcount'])
                progmon.logsecondary_completion(1)
            if nonecnt == args.processes:
                break
        time.sleep(.1)

    workq.join()

    print(' '*70, end='\r')
    print(' Post-scanning PDFs', end='\r')

    outpdfs = gather_pdfs(args.dest)
    outfinfos = [gen_fileinfo(filepath, temproot, args.source, args.dest) for filepath in outpdfs]
    outfinfos, outtotpages, errcnt = pagecount_parallel(outfinfos)

    print('-'*100, '\n')
    if totpages != outtotpages:
        print("Warning, %s pages were counted on the input and %s on the output.  Please check for errors\n" % (totpages, outtotpages))
    if errq.qsize() > 0:
        print("Warning, %s PDFs where skipped because of errors.  Check CleanerErrors.log in the output for details\n" % errq.qsize())
        with open(joinp(args.dest, 'CleanerErrors.log'), 'w') as errlog:
            while errq.qsize() > 0:
                errfile = errq.get()
                errlog.write('!Errors for %s\n\n' % errfile['subpath'])
                errlog.write(errfile['errors'])
                errlog.write('\n--------------------------------------------\n')

    shutil.rmtree(temproot)
    print('Complete. %s docs, %s pages processed in %s\n' % (progmon.secondarymetric_total,
                                                              progmon.primarymetric_total,
                                                              progmon.elapsed()))
    print('-'*100)
