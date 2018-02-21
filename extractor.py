from __future__ import unicode_literals, division

import spacy
import sys
import argparse
import bz2
import fileinput
import os.path
import re
import ujson
from io import StringIO
from toolz import partition
from timeit import default_timer


# ===========================================================================
pre_format_re = re.compile(r'^[\`\*\~]')
post_format_re = re.compile(r'[\`\*\~]$')
url_re = re.compile(r'\[([^]]+)\]\(%%URL\)')
link_re = re.compile(r'\[([^]]+)\]\(https?://[^\)]+\)')

# ===========================================================================
LABELS = {
    'ENT': 'ENT',
    'PERSON': 'ENT',
    'NORP': 'ENT',
    'FAC': 'ENT',
    'ORG': 'ENT',
    'GPE': 'ENT',
    'LOC': 'ENT',
    'LAW': 'ENT',
    'PRODUCT': 'ENT',
    'EVENT': 'ENT',
    'WORK_OF_ART': 'ENT',
    'LANGUAGE': 'ENT',
    'DATE': 'DATE',
    'TIME': 'TIME',
    'PERCENT': 'PERCENT',
    'MONEY': 'MONEY',
    'QUANTITY': 'QUANTITY',
    'ORDINAL': 'ORDINAL',
    'CARDINAL': 'CARDINAL'
}

# ======================================================================

class Extractor(object):
    """
    An extraction task on a article.
    """
    def __init__(self):
        self.nlp = spacy.load('en_core_web_sm')

    def write_output(self, out, text):
        """
        :param out: a memory file
        :param text: the text of the page
        """
        if out == sys.stdout:  # option -a or -o -
            text = text.encode('utf-8')
        out.write(str(text))
        out.write('\n')

    def extract(self, text, out):
        """
        :param out: a memory file.
        """
        text = self.strip_meta(text)

        doc = self.nlp(text)
        text = self.transform_doc(doc)
        self.write_output(out, text)

    def strip_meta(self, text):
        # residuals of unbalanced quotes
        text = link_re.sub(r'\1', text)
        text = text.replace('&gt;', '>').replace('&lt;', '<')
        text = pre_format_re.sub('', text)
        text = post_format_re.sub('', text)
        text = text.replace('\\', '')
        return text


    def transform_doc(self, doc):
        for ent in doc.ents:
            ent.merge(tag=ent.root.tag_, lemma=ent.text, ent_type=LABELS[ent.label_])
        for np in doc.noun_chunks:
            while len(np) > 1 and np[0].dep_ not in ('advmod', 'amod', 'compound'):
                np = np[1:]
            np.merge(tag=np.root.tag_, lemma=np.text, ent_type=np.root.ent_type_)
        strings = []
        for sent in doc.sents:
            if sent.text.strip():
                strings.append(' '.join(self.represent_word(w) for w in sent if not w.is_space))
        if strings:
            return '\n'.join(strings) + '\n'
        else:
            return ''

    def represent_word(self, word):
        if word.like_url:
            return '%%URL|X'
        text = re.sub(r'\s', '_', word.text)
        tag = LABELS.get(word.ent_type_, word.pos_)
        if not tag:
            tag = '?'
        return text + '|' + tag

# ----------------------------------------------------------------------
# Output


class NextFile(object):
    """
    Synchronous generation of next available file name.
    """

    filesPerDir = 100

    def __init__(self, path_name):
        self.path_name = path_name
        self.dir_index = -1
        self.file_index = -1

    def __next__(self):
        self.file_index = (self.file_index + 1) % NextFile.filesPerDir
        if self.file_index == 0:
            self.dir_index += 1
        dirname = self._dirname()
        if not os.path.isdir(dirname):
            os.makedirs(dirname)
        return self._filepath()

    next = __next__

    def _dirname(self):
        char1 = self.dir_index % 26
        char2 = self.dir_index // 26 % 26
        return os.path.join(self.path_name, '%c%c' % (ord('A') + char2, ord('A') + char1))

    def _filepath(self):
        return '%s/reddit_%02d' % (self._dirname(), self.file_index)


class OutputSplitter(object):
    """
    File-like object, that splits output to multiple files of a given max size.
    """

    def __init__(self, nextFile, max_file_size=0, compress=True):
        """
        :param nextFile: a NextFile object from which to obtain filenames
            to use.
        :param max_file_size: the maximum size of each file.
        :para compress: whether to write data with bzip compression.
        """
        self.nextFile = nextFile
        self.compress = compress
        self.max_file_size = max_file_size
        self.file = self.open(next(self.nextFile))

    def reserve(self, size):
        if self.file.tell() + size > self.max_file_size:
            self.close()
            self.file = self.open(next(self.nextFile))

    def write(self, data):
        # self.reserve(len(data))
        self.file.write(data)

    def close(self):
        self.file.close()

    def open(self, filename):
        if self.compress:
            return bz2.BZ2File(filename + '.bz2', 'w')
        else:
            return open(filename, 'wb')


# ----------------------------------------------------------------------

def extraction_process(input_file, out_file, file_size, batch=None):
    print("Starting page extraction from {}.".format(input_file))
    extract_start = default_timer()

    if input_file and batch:
        comments = batch
    else:
        input = fileinput.FileInput(input_file, openhook=fileinput.hook_compressed)
        comments = text_from(input)
        print('Total comments #{}'.format(len(comments)))
    if out_file:
        nextFile = NextFile(out_file)
        output = OutputSplitter(nextFile, file_size)

    out = StringIO()  # memory buffer
    e = Extractor()
    i = 1
    for comment in comments:
        if i < 279136:
            i += 1
            continue
        print(comments)
        e.extract(comment, out)
        text = out.getvalue()
        output.write(text.encode('utf-8'))
        out.truncate(0)
        out.seek(0)
        print("Finished processing #{}".format(i))
        i += 1

    out.close()
    input.close()

    if output != sys.stdout:
        output.close()

    extract_duration = default_timer() - extract_start
    print('Finished extraction of all comments in {}'.format(extract_duration))

# ----------------------------------------------------------------------
def text_from(file_):
    comments = []
    for i, line in enumerate(file_):
        try:
            text = ujson.loads(line)
            comments.append(text.get('body'))
        except:
            continue
    return comments


def iter_comments(file_):
    for i, line in enumerate(file_):
        yield ujson.loads(line)['body']

# ----------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(prog=os.path.basename(sys.argv[0]),
                                     formatter_class=argparse.RawDescriptionHelpFormatter,
                                     description=__doc__)
    parser.add_argument("input",
                        help="XML wiki dump file")
    groupO = parser.add_argument_group('Output')
    groupO.add_argument("-o", "--output", default="text",
                        help="directory for extracted files (or '-' for dumping to stdout)")
    parser.add_argument("--batch", type=bool, default=False,
                        help="Process input file in batches")

    args = parser.parse_args()
    input_file = args.input
    batch_input = args.batch

    output_path = args.output
    if output_path != '-' and not os.path.isdir(output_path):
        try:
            os.makedirs(output_path)
        except:
            return
    # Minimum size of output files
    file_size = 2000 * 1024

    if not batch_input:
        extraction_process(input_file, output_path, file_size)
    else:
        t1 = default_timer()
        batches = partition(2000000, iter_comments(input_file))
        for i, batch in enumerate(batches):
            extraction_process(input_file, output_path, file_size, batch)
            print('Batch# {} is completed!'.format(i))
        t2 = default_timer()
        print("Total time: %.3f" % (t2 - t1))



if __name__ == '__main__':
    main()
