import csv
import os
import sys
import string
import itertools
import subprocess
import glob
import xlsxwriter
import codecs
import pprint as pp


def gather_files(targetdir, filtr=''):
    def gather_windows(targetdir, filtr):
        result = []
        if filtr:
            targetdir = targetdir.rstrip('\\"') + '\\' + filtr
        targetdir = targetdir.rstrip('\\"')
        dirargs = ['dir', targetdir, '/S', '/B', '/A-D']
        proc = subprocess.Popen(dirargs, shell=True, stdout=subprocess.PIPE, bufsize=1)
        for dirline in iter(proc.stdout.readline, b''):
            result.append(dirline.strip())
        proc.communicate()
        return result

    def gather_linux(targetdir, filtr=None):
        result = []
        if not filtr:
            filtr = '*.*'
        for root, dirs, files in os.walk(targetdir):
            globstr = os.path.join(root, filtr)
            result.extend(glob.glob(globstr))
        return result

    if os.name is 'nt':
        return gather_windows(targetdir, filtr)
    else:
        return gather_linux(targetdir, filtr)


def tokenize_txt(txtpath):
    with codecs.open(txtpath, encoding='utf8') as infile:
        raw_tokens = infile.read().split()
    #Use translate to remove digits and punctuation
    #transmap = string.maketrans(string.letters, string.letters)
    #delchars = string.digits + string.punctuation.replace('-', '')
    delmap = {ord(char): None for char in string.digits + string.punctuation.replace('-', '')}  # unicode mapping
    clean_tokens = [token.translate(delmap).lower() for token in raw_tokens]
    return raw_tokens, clean_tokens


def position_match(term_positions):
    def contiguous(pos_set):
        for x in enumerate(pos_set):
            try:
                if x[1] + 1 != pos_set[x[0] + 1]:
                    return False
            except IndexError:
                pass
        return True

    pos_sets = itertools.product(*term_positions)
    return [pos_set for pos_set in pos_sets if contiguous(pos_set)]


def gen_snippet(search_text, raw_tokens, clean_tokens, width=10):
    search_text = search_text.lower().split()
    term_pos = []
    for term in search_text:
        positions = [t[0] for t in enumerate(clean_tokens) if t[1] == term]
        term_pos.append(positions)
    final_postitions = position_match(term_pos)
    snippets = []
    for pos in final_postitions:
        #print pos[0], pos[-1]
        lower = pos[0] - width if pos[0] - width > 0 else 0
        upper = pos[-1] + width + 1 if pos[-1] + width < len(raw_tokens) else len(raw_tokens)
        #print lower, upper, '\n'
        snippets.append(Snippet(raw_tokens[lower:upper],
                                search_text=' '.join(search_text),
                                width=width,
                                term_pos=[p - lower for p in pos]))  # Realign the position for the smaller snippet
    return snippets


class Snippet(object):
    def __init__(self, snip, search_text='', width=10, term_pos=(0, 0, 0)):
        self.snip = snip
        self.search_text = search_text
        self.width = width
        self.search_tokens = search_text.lower().split()
        self.term_pos = term_pos

    def __str__(self):
        return ' '.join(self.snip)

    def __repr__(self):
        return ' '.join(self.snip)

    def term_highlight(self, excel_format=None):
        """
        Retrun the arguments to be used with xlsxwriter write_rich_string function to format
        just the search terms in excel.
        """
        result = [' '.join(self.snip[:self.term_pos[0]])]
        result.append(excel_format)
        result.append(self.search_text)
        result.append(' '.join(self.snip[self.term_pos[-1]+1:]))
        return result

class Excelfile(object):
    """
    Implements a context manager for xlsxwriter excel files.
    """
    def __init__(self, filename='excel.xlsx'):
        self.filename = filename

    def __enter__(self):
        self.workbook = xlsxwriter.Workbook(self.filename)
        return self.workbook

    def __exit__(self, type, value, traceback):
        self.workbook.close()


if __name__ == '__main__':
    cnt = 0
    rowcnt = 1
    inpath = sys.argv[1]
    txtfiles = gather_files(inpath)
    txtfiles = [(os.path.split(os.path.splitext(p)[0])[1], p) for p in txtfiles]

    with Excelfile('test.xlsx') as excel:
        ws = excel.add_worksheet()
        bold = excel.add_format({'bold': True})
        ws.write('A1', 'DocID')
        ws.write('B1', 'Snippet')
        for docid, txtfile in txtfiles:
            cnt += 1
            if cnt % 30 == 0: print cnt
            print docid
            rawt, cleant = tokenize_txt(txtfile)
            snippets = gen_snippet('Due Diligence', rawt, cleant)
            for snip in snippets:
                rowcnt += 1
                cellA, cellB = 'A' + str(rowcnt), 'B' + str(rowcnt)
                ws.write(cellA, docid)
                ws.write_rich_string(cellB, *snip.term_highlight(bold))


