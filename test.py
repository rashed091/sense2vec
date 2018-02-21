from __future__ import print_function, unicode_literals, division
# import sense2vec
import spacy


# model = sense2vec.load()
# freq, query_vector = model["natural_language_processing|NOUN"]
# print(model.most_similar(query_vector, n=3))

nlp = spacy.load('en_core_web_sm')
