from __future__ import print_function, unicode_literals, division

import glob
import os
import io
import re
import uuid
import shutil
from timeit import default_timer
from multiprocessing import cpu_count

import ujson
import spacy
from toolz import partition_all
from joblib import Parallel, delayed
from settings import OUTPUT_DIR, INPUT_DIR



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


def parallelize(func, iterator, n_jobs, extra):
    extra = tuple(extra)
    return Parallel(n_jobs=n_jobs)(delayed(func)(*(item + extra)) for item in iterator)


def read_files(loc):
    with io.open(loc, encoding='utf-8') as file_:
        for i, line in enumerate(file_):
            yield line


def strip_meta(text):
    pre_format_re = re.compile(r'^[\`\*\~]')
    post_format_re = re.compile(r'[\`\*\~]$')
    link_re = re.compile(r'\[([^]]+)\]\(https?://[^\)]+\)')

    text = link_re.sub(r'\1', text)
    text = text.replace('&gt;', '>').replace('&lt;', '<')
    text = pre_format_re.sub('', text)
    text = post_format_re.sub('', text)
    return text


def parse_and_transform(batch_id, input_, out_dir, temp_dir):
    out_loc = os.path.join(out_dir, '%d.txt' % batch_id)
    if os.path.exists(out_loc):
        return None

    print('Batch', batch_id)
    nlp = spacy.load('en_core_web_sm', disable=['textcat'])

    temp_loc = os.path.join(temp_dir, uuid.uuid4().hex)
    
    with io.open(temp_loc, 'w', encoding='utf8') as file_:
        for text in input_:
            try:
                doc = nlp(text)
                file_.write(transform_doc(doc))
            except:
                continue

        file_.close()
        shutil.copy(temp_loc, out_loc)
        os.remove(temp_loc)


def transform_doc(doc):
    for ent in doc.ents:
        ent.merge(tag=ent.root.tag_, lemma=ent.text, ent_type=LABELS[ent.label_])
    for np in doc.noun_chunks:
        while len(np) > 1 and np[0].dep_ not in ('advmod', 'amod', 'compound'):
            np = np[1:]
        np.merge(tag=np.root.tag_, lemma=np.text, ent_type=np.root.ent_type_)
    strings = []
    for sent in doc.sents:
        if sent.text.strip():
            strings.append(' '.join(represent_word(w) for w in sent if not w.is_space))
    if strings:
        return '\n'.join(strings) + '\n'
    else:
        return ''


def represent_word(word):
    if word.like_url:
        return '%%URL|X'

    text = re.sub(r'\s', '_', word.text)
    tag = LABELS.get(word.ent_type_, word.pos_)
    if not tag:
        tag = '?'
    return text + '|' + tag


def create_dir_if_not_exists(dir_path):
    if not os.path.isdir(dir_path):
        try:
            original_umask = os.umask(0)
            os.mkdir(dir_path)

        except Exception as e:
            print(e)
        finally:
            os.umask(original_umask)


def process_file(file_name):
    in_loc = os.path.join(INPUT_DIR, '{}'.format(file_name))
    jobs = partition_all(10000, read_files(in_loc))

    worker = max(1, cpu_count() - 2)

    output_path = os.path.join(OUTPUT_DIR, file_name)
    temp_path = os.path.join(OUTPUT_DIR, "__temp__")

    create_dir_if_not_exists(output_path)
    create_dir_if_not_exists(temp_path)

    start_time = default_timer()
    parallelize(parse_and_transform, enumerate(jobs), worker, [output_path, temp_path])
    end_time = default_timer()

    print('Execution Time: {}'.format(end_time - start_time))


if __name__ == '__main__':
    for file_name in glob.glob(INPUT_DIR + '/*'):
        name = file_name.split('/')[-1]
        if name == 'README.md':
            continue
        process_file(name)


