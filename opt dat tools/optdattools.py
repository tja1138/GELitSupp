#!/usr/bin/python

#Library for parsing and storing opticon and Concordance dat files.
#Special processing for when these are loan file based.
#ver: 0.1

#import sys
#import os.path
import csv
import os
import os.path


class OptRecord:
    """
    Data structure of a document in an opt with associated metadata.

    Fields/init args:
     pages- list of rows from the opt file. Each row is also a list of
                 the fields in that row, e.g. pages[0][0] is the begdoc of the
                 first page of the row.
    """
    def __init__(self, pages):
        self.begdoc = pages[0][0]
        self.pages = pages
        self.pgcount = len(pages)


class OptFile(object):
    """Store a representation of an opticon file, dict of OptRecords keyed on
    begdoc or docid
    """
    def __init__(self):
        """Create empty dict for documents."""
        self.docrecords = {}
        super(OptFile, self).__init__()

    def load_opt(self, optpath, pathroot=None):
        """Loads a opticon file into the internal dict.

        Args:
            optpath- file path to the target opticon file.
            pathroot- if supplied joins the file path in each line of the opt
                        to this root before storing it.

        """
        currlines = []  # per loop storage of the current docs lines
        with open(optpath, 'r') as optfh:
            # TODO: add first line checking here.
            firstline = optfh.readline().split(',')
            if pathroot is not None:
                firstline[2] = self._joinpath(pathroot, firstline[2])
            currlines.append(firstline)

            for line in optfh.readlines():
                linesp = line.split(',')
                if pathroot is not None:
                    linesp[2] = self._joinpath(pathroot, linesp[2])
                if linesp[3] == 'Y':  # if new opt doc store current lines
                    self.docrecords[currlines[0][0]] = OptRecord(currlines)
                    currlines = []
                    currlines.append(linesp)
                else:
                    currlines.append(linesp)  # if not a new doc keep adding

            self.docrecords[currlines[0][0]] = OptRecord(currlines)

    def _joinpath(self, root, path):
        if os.sep == '/':  # on linux
            path = path.replace('\\', '/').strip('./')  # change windows path to linux
            return os.path.join(root, path)
        else:  # Assume on Windows
            path = path.strip('.\\')
            return os.path.join(root, path)

    #magic functions to give [] access to the docrecords dict.

    def __len__(self):
        return len(self.docrecords)

    def __getitem__(self, key):
        return self.docrecords[key]

    def __setitem__(self, key, value):
        if isinstance(value, OptRecord):
            self.docrecords[key] = value
        else:
            print "gaaa, invalid insertion", value.__class__
            pass  # TODO: add error raise for invalid type

    def __iter__(self):
        return self.docrecords.iterkeys()


class DatFile(object):
    """Stores a representation of a Concordance dat in a main dict, keyed on
    an index field.  Each record is stored as a sub-dict.
    """
    def __init__(self):
        self.datrecords = {}
        self.header_row = []
        super(DatFile, self).__init__()

    def load_dat(self, datpath, index_header):
        """Load a dat into the object.

        Args:
            datpath- path to the dat file to be loaded.
            index_header- Header column of dat that will be the lookup index.

        """
        with open(datpath) as datfh:
            datreader = csv.reader(datfh, delimiter=chr(20), quotechar=chr(254))
            self.header_row = datreader.next()
            self.index_fieldname = index_header
            dictidx = self.header_row.index(index_header)
            for row in datreader:
                d = {}
                for i, item in enumerate(row):
                    d[self.header_row[i]] = item
                self.datrecords[row[dictidx]] = d
                #self.datrecords[row[dictidx]] = row

    def __len__(self):
        return len(self.datrecords)

    def __getitem__(self, key):
        return self.datrecords[key]

    def __setitem__(self, key, value):
        if isinstance(value, list):
            self.datrecords[key] = value
        else:
            print "gaaa, invalid insertion", value.__class__
            pass  # TODO: add error raise for invalid type

    def __iter__(self):
        return self.datrecords.iterkeys()


class LoanFileList(OptFile, DatFile):
    """Object containing loan file info with their complete dcouments
    lists and page paths. Also includes utilities for gathering
    basic info about them"""
    def __init__(self):
        super(LoanFileList, self).__init__()

    def gather_loans_byfield(self, loanfield, filenamefield=None):
        """Merge loan, document, and image info into one dictionaty tree rooted at
        the loan number.  This variate uses a loan id column taken from the dat file.

        Args:
            loanfield - (Required) Field heading from the dat that contains the loan number.
            filenamefield - Original file name field from the dat, if any.
        """
        #set comprehension to get unique loan numbers
        self.filenamefield = filenamefield
        loanset = {v[loanfield] for v in self.datrecords.viewvalues()}
        docpairs = [(v[self.index_fieldname], v[loanfield])
                    for v in self.datrecords.viewvalues()]

        self.loanindex = self._loanmatch(loanset, docpairs)

    def _loanmatch(self, loannums, docs):
        """Associate loan numbers with the list of docs in that loan.
        Not very pythonic I know, but so much faster than the old way.
        """
        loannums = sorted(loannums)
        docs = sorted(docs, key=lambda x: x[0])
        docs = sorted(docs, key=lambda x: x[1])

        currdocdict = {}
        out = {}
        i = 0
        for loan in loannums:
            while True:
                try:
                    if loan == docs[i][1]:
                        currdocdict[docs[i][0]] = self._docdetails(docs[i][0],
                                                              self.filenamefield)
                        i += 1
                    else:
                        out[loan] = currdocdict
                        currdocdict = {}
                        break
                except IndexError:  # i has fallen off the end of docs so return
                    out[loan] = currdocdict
                    return out

    def _docdetails(self, docnum, filenamef):
        """Gather additional document details to a dict.
        """
        out = {}
        if filenamef is not None:
            out['filename'] = self.datrecords[docnum][filenamef]
        else:
            out['filename'] = None
        out['images'] = self.docrecords[docnum].pages
        return out

if __name__ == "__main__":
    import time

    st = time.time()

    testloan = LoanFileList()
    testloan.load_opt('WAMU 2004-AR12_001.opt', r'c:\usr\tom\testdata')
    print 'Opt load at %d:%.1f' % divmod(time.time() - st, 60)
    testloan.load_dat('WAMU 2004-AR12_001.dat', 'BegBates')
    print 'Dat load at %d:%.1f' % divmod(time.time() - st, 60)
    #print repr(sorted(testopt.docrecords.keys()))
    #for line in testloan.docrecords['JPMC_DBNTC_LF0041104319'].pages:
        #print repr(line)
    #print testloan.datrecords['JPMC_DBNTC_LF0041104319']

    testloan.gather_loans_byfield(loanfield="LoanNumber", filenamefield='Document Type')
    print 'loan gather at %d:%.1f' % divmod(time.time() - st, 60)
    import pprint as pp
    pp.pprint(sorted(testloan.loanindex['601245723'].viewkeys()))
    print '\n'
    pp.pprint(testloan.loanindex['601245723']['JPMC_DBNTC_LF0041104310']['images'])
    print '\n', testloan.loanindex['601245723']['JPMC_DBNTC_LF0041104310']['filename']


    #for x in testloan.loanindex['602168650']:
        #print x
    #print len(testloan.loanset)