# -*- coding: utf-8 -*-
#Scans an input directory for pdf loan files and produces a page count report.Scans
from __future__ import print_function
import sys
import subprocess
import os
#import re
import string
from collections import defaultdict


def genfilelist(root, minlen=5, maxlen=200, foldonly=False):
    '''
    Returns a dict table of loan numbers and associated paths to pdfs for all pdfs found
    on the path root.  Loan numbers are extracted from the file path.  Maximum and minimum
    loan number lengths to search for can me set.
    '''
    loantable = defaultdict(list)
    splitchars = list(string.ascii_letters) + list(string.punctuation) + list(string.whitespace)
    try:
        os.chdir(root)
    except:
        print("Error: target directory not found\n")
        sys.exit(1)
    proc = subprocess.Popen(['dir', '*.pdf', '/S', '/B'],
                            shell=True, stdout=subprocess.PIPE, bufsize=1)
    for line in iter(proc.stdout.readline, b''):
        if foldonly is True:
            line = os.sep.join(line.split(os.sep)[:-1])
        possiblenums = set([num for num in splitbyl(line, splitchars)
                            if minlen <= len(num) <= maxlen])
        if len(possiblenums) < 1:
            continue
        #if there are multiple possible loan numbers pick the longest one.
        loannum = sorted(possiblenums, key=lambda x: len(x), reverse=True)[0]
        loantable[loannum].append(line.strip())
    proc.communicate()
    return loantable


def countpages(loannum, pdflist):
    totaldocs = len(pdflist)
    totalpages = 0
    for pdfpath in pdflist:
        try:
            dump = subprocess.check_output(["pdfinfo", pdfpath], shell=True,
                                            stderr=subprocess.STDOUT)
            dumpsplit = dump.split()
            totalpages += int(dumpsplit[dumpsplit.index('Pages:') + 1])
        except subprocess.CalledProcessError:
            pass
    return (loannum, totaldocs, totalpages)


def splitbyl(txt, seps):
    default_sep = seps[0]
    for sep in seps[1:]:  # we skip seps[0] because that's the default seperator
        txt = txt.replace(sep, default_sep)
    return txt.split(default_sep)


if __name__ == '__main__':
    #import pprint as pp

    import csv
    import time

    starttime = time.time()
    results = []
    rootpath = sys.argv[1]
    pdftable = genfilelist(rootpath)

    ctime = '%d:%.1f' % divmod(time.time() - starttime, 60)
    print('Dir listing complete:', ctime, '\n')

    loancnt = len(pdftable)
    progcnt = 0

    for loan, pdflist in pdftable.viewitems():
        out = countpages(loan, pdflist)
        results.append(out)
        progcnt += 1
        print('%s / %s counted' % (progcnt, loancnt), end='\r')

    ctime = '%d:%.1f' % divmod(time.time() - starttime, 60)
    print('Count complete:', ctime)

    totalpdfs = sum([x[1] for x in results])
    totalpages = sum([x[2] for x in results])
    results.insert(0, ('Totals', totalpdfs, totalpages))

    try:
        os.chdir(rootpath)
    except:
        print("Error: target directory not found\n")
        sys.exit(1)

    with open('LoanPageReportI.csv', 'wb') as outcsv:
        report = csv.writer(outcsv, dialect='excel')
        report.writerow(['Loan Number', 'File Count', 'Page Count'])
        report.writerows(results)

    ctime = '%d:%.1f' % divmod(time.time() - starttime, 60)
    print('Complete:', ctime)