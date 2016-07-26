import logging
from rdflib.plugins.parsers.ntriples import NTriplesParser, Sink
from rdflib.util import *
from scipy import *
import re
import sys

class CountSink(Sink):
    res = set()
    filedict = {}

    def triple(self, s, p, o):
        self.res.add(s)
        self.res.add(o)
        if p not in self.filedict:
            ps = str(p)
            l = ps.rfind("/")
            self.filedict[p] = open("graph/{}.edgelist".format(ps[l+1:]), 'w')


class RDFToTensorSink(Sink, dict):
    i = 0
    length = 0
    res = {}
    tensor_size = 0
    slice_collection = list()
    filedict = {}

    def set_filedict(self, filedict):
        self.filedict = filedict

    def triple(self, s, p, o):
        try:
            self.length += 1
            if s not in self.res:
                self.res[s] = len(self.res)
            if o not in self.res:
                self.res[o] = len(self.res)
            index_s = self.res[s]
            index_o = self.res[o]
            self.filedict.get(p).write("{} {}\n".format(index_s, index_o))
            print "{} {} {} --> {} {}".format(s, p, o, index_s, index_o)
        except UnicodeEncodeError:
            print "Unicode error, skipping triple..."
            self.i += 1


# set logging to basic
logging.basicConfig()

pathToFile = sys.argv[1]
targetDir = "graph"

csk = CountSink()
ntp = NTriplesParser(csk)
with open(pathToFile, "r") as anons:
    print "Counting into {}...".format(pathToFile)
    ntp.parse(anons)

f = open(targetDir + '/resources.tsv', 'w')
for r in csk.res:
    f.write(re.sub(r"\n", " ", re.sub(r"\r", " ", r.n3().encode('utf8')[1:-1])) + "\n")

sk = RDFToTensorSink()
sk.set_filedict(csk.filedict)
sk.tensor_size = len(csk.res)
n = NTriplesParser(sk)
with open(pathToFile, "r") as anons:
    print "Extracting relationships from {}...".format(pathToFile)
    n.parse(anons)
print "triples = {}, errors = {}".format(sk.length, sk.i)

print "Done!"
