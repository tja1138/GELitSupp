from __future__ import print_function
from ctypes import c_int
import sys
import multiprocessing
import time
import os
import copy
import os.path
import subprocess
import csv
import pprint as pp

autobahn_cmd_template = ['autobahndx',
                        '/Operation="mergetifftopdf"',
                        '/SourceType="folder"',
                        '/SplitBy="Single"',
                        '/TextFile="Plain Text"',
                        '/AutoRotate="false"',
                        '/Despeckle="0"',
                        '/ExtractImages="Auto"',
                        '/CreateFolders="true"',
                        '/PDFA="false"',
                        '/JBIG2="false"',
                        '/MRC="false"',
                        '/Deskew="false"',
                        '/OnErrorContinue="true"',
                        '/Language="0"',
                        '/OCR="true"',
                        '/GraphicsProc="Remove Lines in OCR Processing"',
                        '/TIFFCompression="Group4"',
                        '/PDFDPI="Auto"',
                        '/TIFFResolution="200"',
                        '/Errors="C:\Aquaforest\Autobahn DX\work\\0000\errors"',
                        '/RetainMetadata="false"',
                        '/RetainBookmarks="false"',
                        '/NonImagePDF="OCR"',
                        '/OCRTextFile="false"',
                        '/Strength="None"',
                        '/PageLayoutSinglePage="Single Page"',
                        '/PageModeUseNone="Neither Bookmarks nor Thumbnails Open"',
                        '/HideToolBar="false"',
                        '/HideMenuBar="false"',
                        '/HideWindowUI="false"',
                        '/FitWindow="false"',
                        '/CenterWindow="false"',
                        '/Convertbookmarks="false"',
                        '/Bookmarkdepth="Heading 1"',
                        '/Converthyperlinks="false"',
                        '/PrintAllSheets="false"',
                        '/PrintbgColor="false"',
                        '/ImageCompression="COMPRESS_JPEG"',
                        '/ImageDownsizing="false"',
                        '/FontSubstitution="SUBST_NONE"',
                        '/FontEmbedding="EMBED_FULL"',
                        '/FontEmbedAsType0="false"',
                        '/PaperOrientation="ORIENT_LANDSCAPE"',
                        '/FontEmbeddingIsSet="true"',
                        '/FontSubstitutionIsSet="true"',
                        '/PaperOrientationIsSet="true"',
                        '/OCRGraphicZones="false"',
                        '/keepimages="false"']

                        #/Target="C:\matters\Comp sys\\autobhan"
                        #/Source="C:\matters\Comp sys\\autobhan\\test loans\small"
                        #/Output="%DIRNAME_merge.pdf"


class OcrWorker(multiprocessing.Process):
    def __init__(self, workq, args=None):
        super(OcrWorker, self).__init__()
        self.workq = workq
        self.args = args

    def run(self):
        autobahn_cmd = copy.copy(autobahn_cmd_template)
        while True:
            source = self.workq.get()
            if source is None:
                #with self.args['stdout_lock']:
                #    print('%s: Exiting' % self.name)
                self.workq.task_done()
                break
            outname = source.split('\\')[-1].strip()
            autobahn_cmd.insert(2, ('/Output="%s_merge.pdf"' % outname))
            autobahn_cmd.insert(2, ('/Target="%s"' % self.args['target_dir']))
            autobahn_cmd.insert(2, ('/Source="%s"' % source))
            proc = subprocess.Popen(autobahn_cmd, shell=True, stdout=subprocess.PIPE, bufsize=1)
            for resline in iter(proc.stdout.readline, b''):
                if 'complete' in resline:
                    with self.args['curr_cnt_lock']:
                        self.args['curr_cnt'].value += 1
                    with self.args['running_cnt_lock']:
                        self.args['running_cnt'].value += 1
                    #with self.args['stdout_lock']:
                        #print('%s -> %s' % (self.name, resline.strip()))
            proc.communicate()
            self.workq.task_done()
            autobahn_cmd = copy.copy(autobahn_cmd_template)


def reporter(workq, args):
    from collections import deque
    hist = deque(maxlen=30)
    while True:
        if int(workq.qsize()) == 0:
            break
        with args['curr_cnt_lock']:
            args['curr_cnt'].value = 0
        time.sleep(5)
        with args['curr_cnt_lock']:
            hist.append(args['curr_cnt'].value)
        sec_rate = float(sum(hist)) / (len(hist) * 5)
        hour_rate = sec_rate * 3600
        with args['stdout_lock']:
            with args['running_cnt_lock']:
                if sec_rate > 0:
                    remaining_sec = (args['total_pgcount'] - args['running_cnt'].value) / sec_rate
                    remaining = '%d:%.1f' % divmod(remaining_sec, 60)
                else:
                    remaining = 'inf.'
                print(' ' * 60, '\r', '  %s workers :: %s pg/hr :: %s/%s :: %s' %
                     (args['numwokers'], hour_rate, args['running_cnt'].value,
                      args['total_pgcount'], remaining), end='\r')


if __name__ == '__main__':

    with open(sys.argv[1]) as fh:
        indirs = fh.readlines()
    indirs = [d.strip() for d in indirs]
    outdir = sys.argv[2]
    results = [['worker', 'total pages', 'time', 'pg/hour']]
    args = {}
    args['pgcount'] = 30
    args['stdout_lock'] = multiprocessing.Lock()
    args['curr_cnt_lock'] = multiprocessing.Lock()
    args['running_cnt_lock'] = multiprocessing.Lock()
    args['curr_cnt'] = multiprocessing.Value(c_int)
    args['running_cnt'] = multiprocessing.Value(c_int)
    args['target_dir'] = sys.argv[2]
    print('\n')

    #master loop
    for numworker in xrange(1, 5):
        args['total_pgcount'] = args['pgcount'] * numworker * 2
        targetdirs = copy.copy(indirs)[:numworker * 2]
        args['numwokers'] = numworker
        args['running_cnt'].value = 0
        #print('starting num:', numworker, '\n')
        workq = multiprocessing.JoinableQueue()
        reporter_proc = multiprocessing.Process(target=reporter, args=(workq, args))
        workers = [OcrWorker(workq, args) for i in xrange(numworker)]
        for worker in workers:
            worker.start()

        masterstart = time.time()
        for targetdir in targetdirs:
            workq.put(targetdir)

        # Add a poison pill for each consumer
        for i in xrange(numworker):
            workq.put(None)

        reporter_proc.start()
        workq.join()

        elapsed_time = time.time() - masterstart
        rate = (args['total_pgcount'] / elapsed_time) * 3600
        results.append([numworker, args['total_pgcount'], elapsed_time, rate])

        #cleanup
        walker = os.walk(outdir)
        first_walk = next(walker)
        files = [os.path.join(first_walk[0], fname) for fname in first_walk[2]]
        for f in files:
            os.remove(f)
        reporter_proc.join()

    #write results
    with open(sys.argv[3], 'wb') as outfh:
        writer = csv.writer(outfh)
        writer.writerows(results)
    print('complete')
