# -*- coding: utf-8 -*-
from __future__ import print_function
import loanpdfcounter as lpc
import sys
import multiprocessing
import os


class Worker(multiprocessing.Process):

    def __init__(self, workq, resultq, args):
        super(Worker, self).__init__()
        self.workq = workq
        self.resultq = resultq
        self.args = args

    def run(self):
        while True:
            loannum, pdflist = self.workq.get()
            if loannum is None:  # the poison pill
                if self.args.debug:
                    self.args.stdout_lock.acquire()
                    print('%s: Exiting' % self.name)
                    self.args.stdout_lock.release()
                self.workq.task_done()
                break
            if self.args.debug:
                self.args.stdout_lock.acquire()
                print('%s counting %s: %s' % (self.name, loannum, self.workq.qsize()))
                self.args.stdout_lock.release()
            else:
                self.args.stdout_lock.acquire()
                print('  %s / %s loans counted' %
                     (self.resultq.qsize(), self.args.loancnt), end='\r')
                self.args.stdout_lock.release()
            out = lpc.countpages(loannum, pdflist)
            self.workq.task_done()
            self.resultq.put(out)
        return


if __name__ == '__main__':
    import csv
    import time
    import argparse
    import itertools

    parser = argparse.ArgumentParser(description=
                        'Count pdf pages in loan file and produce a by loan report')
    parser.add_argument('rootpath', action='store',
                        help='folder root to search for pdfs')
    parser.add_argument('-min', action='store', dest='minlen', type=int, default=5,
                        help='minmum length of loan number to find')
    parser.add_argument('-max', action='store', dest='maxlen', type=int, default=200,
                        help='maximum length of loan number to find')
    parser.add_argument('-folder', action='store_true', default=False,
                        help='ignore filename for finding loan numbers, flag')
    parser.add_argument('-p', action='store', dest='num_workers',
                        type=int, default=multiprocessing.cpu_count(),
                        help='number of concurrent processes, default core count')
    parser.add_argument('-debug', action='store_true', default=False,
                        help='print verbose debug output, flag')

    args = parser.parse_args()

    starttime = time.time()
    results = []
    errors = []
    rootpath = args.rootpath
    num_workers = args.num_workers
    print('\n  Scanning directory, gathering loan numbers', end='\r')
    pdftable = lpc.genfilelist(rootpath, minlen=args.minlen,
                                maxlen=args.maxlen, foldonly=args.folder)
    ctime = '%d:%.1f' % divmod(time.time() - starttime, 60)
    print(' ' * 50, '\r', ' Loan gather complete:', ctime, '\n')

    args.loancnt = len(pdftable)
    args.stdout_lock = multiprocessing.Lock()
    workq = multiprocessing.JoinableQueue()
    resultq = multiprocessing.Queue()
    workers = [Worker(workq, resultq, args) for i in xrange(num_workers)]
    for w in workers:
        w.start()

    for loan, pdflist in pdftable.viewitems():
        workq.put((loan, pdflist))

    # Add a poison pill for each consumer
    for i in xrange(num_workers):
        workq.put((None, None))

    workq.join()
    #try:
        #workq.join()
    #except KeyboardInterrupt:
        #print('doh')
        #for w in workers:
            #w.terminate()
        #sys.exit(1)

    while not resultq.empty():
        loannum, totaldocs, totalpages, errorfiles = resultq.get()
        results.append((loannum, totaldocs, totalpages))
        if len(errorfiles) > 0:
            errors.append(errorfiles)

    ctime = '%d:%.1f' % divmod(time.time() - starttime, 60)
    #print('Count complete:', ctime)

    totalpdfs = sum([x[1] for x in results])
    totalpages = sum([x[2] for x in results])
    results.insert(0, ('Totals', totalpdfs, totalpages))

    try:
        os.chdir(rootpath)
    except:
        print("Error: target directory not found\n")
        sys.exit(1)

    with open('LoanPageReportMP.csv', 'wb') as outcsv:
        report = csv.writer(outcsv, dialect='excel')
        report.writerow(['Loan Number', 'File Count', 'Page Count'])
        report.writerows(results)

    if len(errors) > 0:
        print('Error files not included in count:\n')
        for err in itertools.chain(*errors):
            print(err)

    ctime = '%d:%.1f' % divmod(time.time() - starttime, 60)
    print(' ' * 50, '\r', ' Complete:', ctime, '\n')