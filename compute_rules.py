from pymongo import MongoClient
from bson.code import Code
from os import path
from codecs import encode, decode

conn = MongoClient()
db = conn.foo

def computeRules(sup, conf):
    products = {}
    for prod in db.counts.find():
        products[prod["_id"]] = prod["value"]
    corpusCount = db.corpus.count()
    pairs = db.counts_pairs.find()
    total = 0
    for p in pairs:
        x,y = p["_id"].split(',')
        supportXY = p["value"] / corpusCount * 100
        confidenceXY = p["value"] / products[x] * 100
        confidenceYX = p["value"] / products[y] * 100
        if supportXY > sup:
            if confidenceXY > conf:
                total += 1
                print x, '->', y, '\t', 'sup:', supportXY, 'conf:', confidenceXY
            if confidenceYX > conf:
                total += 1
                print y, '->', x, '\t', 'sup:', supportXY, 'conf:', confidenceXY
    print 'total:', total, '\n\n'

#computeRules(1, 1)
#computeRules(1, 25)
#computeRules(1, 50)
computeRules(1, 75)
computeRules(5, 25)
computeRules(7, 25)
#computeRules(20, 25)
#computeRules(50, 25)



