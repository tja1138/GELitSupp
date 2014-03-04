import csv
import os
import sys
import string
import itertools
import subprocess
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

    def gather_linux(targetdir, filtr):
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
    with open(txtpath) as infile:
        raw_tokens = infile.read().split()
    #Use translate to remove digits and punctuation
    transmap = string.maketrans(string.letters, string.letters)
    delchars = string.digits + string.punctuation
    clean_tokens = [token.translate(transmap, delchars).lower() for token in raw_tokens]
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


def snippet(search_text, raw_tokens, clean_tokens, width=10):
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
        snippets.append(raw_tokens[lower:upper])
    return snippets

if __name__ == '__main__':
    cnt = 0
    inpath = sys.argv[1]
    txtfiles = gather_files(inpath)
    txtfiles = [(os.path.split(os.path.splitext(p)[0])[1], p) for p in txtfiles]

    with open(sys.argv[2], 'wb') as outcsv:
        writer = csv.writer(outcsv, quoting=csv.QUOTE_ALL)
        writer.writerow(['DocID', 'Snippet'])
        for docid, txtfile in txtfiles:
            cnt += 1
            if cnt % 30 == 0: print cnt
            rawt, cleant = tokenize_txt(txtfile)
            snippets = snippet('Due Diligence', rawt, cleant)
            for snip in snippets:
                snip = ' '.join(snip).replace('=', '')
                if snip[0] in '=-+*/^':
                    snip = "'" + snip
                writer.writerow([docid, snip])
