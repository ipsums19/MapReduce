from pymongo import MongoClient
from bson.code import Code
from os import path
from codecs import encode, decode

conn = MongoClient()
db = conn.foo

db.corpus.drop()
db.counts_pairs.drop()
db.counts.drop()

with open("groceries.csv") as f:
    for line in f:
        text = []    
        for word in line.split(','):
          word = decode(word.strip(),'latin2','ignore')
          text.append(word)
        d = {}
        d['content'] = text
        db.corpus.insert(d)


################### pairs  ###################

mapper = Code("""
function() {
    for (var i = 0; i < this.content.length; i++) {
        for (var j = i+1; j < this.content.length; j++) {
               emit(this.content[i].concat(",",this.content[j]),1);
            }
        }
    }
              """)

reducer = Code("""
function(key,values) {
    var total = 0;
    for (var i = 0; i < values.length; i++) {
        total += values[i];
        }
        return total;
    }
               """)  

r = db.corpus.map_reduce(mapper, reducer, "counts_pairs")

################### single  ###################

mapper = Code("""
function() {
    for (var i = 0; i < this.content.length; i++) {
        emit(this.content[i],1);
        }
    }
              """)

reducer = Code("""
function(key,values) {
    var total = 0;
    for (var i = 0; i < values.length; i++) {
        total += values[i];
        }
        return total;
    }
               """)  

r = db.corpus.map_reduce(mapper, reducer, "counts")

