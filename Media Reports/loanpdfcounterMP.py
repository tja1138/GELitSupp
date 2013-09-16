# -*- coding: utf-8 -*-
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
                print '%s: Exiting' % self.name
                self.workq.task_done()
                break
            print '%s counting %s: %s' % (self.name, loannum, self.workq.qsize())
            out = lpc.countpages(loannum, pdflist)
            self.workq.task_done()
            self.resultq.put(out)
        return


if __name__ == '__main__':
    import csv
    import time
    import argparse

    parser = argparse.ArgumentParser(description='Count pdf pages in loan file and produce a by loan report')
    parser.add_argument('rootpath', action='store')
    parser.add_argument('-min', action='store', dest='minlen', type=int, default=5)
    parser.add_argument('-max', action='store', dest='maxlen', type=int, default=200)
    parser.add_argument('-folder', action='store_true', default=False)
    parser.add_argument('-p', action='store', dest='num_workers',
                        type=int, default=multiprocessing.cpu_count())

    args = parser.parse_args()

    starttime = time.time()
    results = []
    rootpath = args.rootpath
    num_workers = args.num_workers
    pdftable = lpc.genfilelist(rootpath, minlen=args.minlen,
                                maxlen=args.maxlen, foldonly=args.folder)
    ctime = '%d:%.1f' % divmod(time.time() - starttime, 60)
    print 'Dir listing complete:', ctime, '\n'

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

    #procs = []
    #for i in xrange(num_workers):
        #p = multiprocessing.Process(target=count_worker, args=(workq, resultq))
        #p.start()
        #procs.append(p)

    #for pp in procs:
        #pp.join()

    while not resultq.empty():
        results.append(resultq.get())

    ctime = '%d:%.1f' % divmod(time.time() - starttime, 60)
    print 'Count complete:', ctime

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

    ctime = '%d:%.1f' % divmod(time.time() - starttime, 60)
    print 'Complete:', ctime