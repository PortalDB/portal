import sys;
sys.path.insert(0, './python')
import os;
import re;
import models;
import subprocess;
from subprocess import Popen, PIPE;

def collect_time(output):
    ftime = atime = stime = ptime = ctime = -1
    
    #collect final runtime
    r = re.compile('Final Runtime: (.*?)ms\n')
    m = r.search(output)

    if m:
       ftime = int(m.group(1))
       print "FTime: ", ftime

    #collect aggregation time
    r = re.compile('Aggregation Runtime: (.*?)ms\n')
    m = r.search(output)

    if m:
       atime = int(m.group(1))
       print "ATime: ", atime

    #collect selection time   
    r = re.compile('Selection Runtime: (.*?)ms\n')
    m = r.search (output)

    if m:
       stime = int(m.group(1))
       print "STime: ", stime    

    #collect pagerank time   
    r = re.compile('PageRank Runtime: (.*?)ms\n')
    m = r.search (output)

    if m:
       ptime = int(m.group(1))
       print "PTime: ", ptime

    #collect count time   
    r = re.compile('Count Runtime: (.*?)ms\n')
    m = r.search (output)

    if m:
       ctime = int(m.group(1))
       print "CTime: ", ctime

#helper fumction for collect_args
def get_agg_type(sem):
    if sem == "universal":
        return 0;
    return 1;   
    
#helper function for collect_args
def create_op(oT, a1, a2, pS, nP, rW):
    op = models.Operation( \
        #opID = _ _ _ (filled in later)
        opType = oT, \
        arg1 = a1, \
        arg2 = a2, \
        partitionS = pS, \
        numParts = nP, \
        runWidth = rW )
    return op

def collect_args(query):
    print query
    line = query.split(" ");
    opDict = {} #dictionary of all operations in the query
    seqNum = 1;

    for i in range (0, len(line)):
        if line[i] == "--agg":
            opType = "Aggegate"
            arg1 = runW = int(line[i+1])
            arg2 = get_agg_type(line[i+2]) 
            partS = numParts = None

            if line[i+3] == "-p":
                partS = line[i+4]
                numParts = int(line[i+5])
            newOp = create_op(opType, arg1, arg2, partS, numParts, runW)
            opDict.update({seqNum: newOp})
            seqNum += 1 
    
        if line[i] == "--select":
            opType = "Select"
            arg1 = int(line[i+1])
            arg2 = int(line[i+2])
            partS = runW = numParts = None

            if line[i+3] == "-p":
                runW = 1 #FIXME: what is runW for selection??
                partS = line[i+4]
                numParts = int(line[i+5])
            newOp = create_op(opType, arg1, arg2, partS, numParts, runW)
            opDict.update({seqNum: newOp})   
            seqNum += 1

        if line[i] == "--pagerank":
            opType = "PageRank"
            arg1 = int(line[i+1])
            arg2 = 1 #FIXME: what is arg2 for pr??
            partS = runW = numParts = None

            if line[i+2] == "-p":
                runW = 1 #FIXME: what is runW for pr??
                partS = line[i+3]
                numParts = int(line[i+4])
            newOp = create_op(opType, arg1, arg2, partS, numParts, runW)
            opDict.update({seqNum: newOp})
            seqNum += 1

        if line[i] == "--count":
            opType = "Count"
            arg1 = arg2 = None # no args for count
            partS = runW = numParts = None            

            if line[i+1] == "-p":
                runW = 1 #FIXME: what is runW for count?
                partS = line[i+2]
                numParts = int(line[i+3]) 
            newOp = create_op(opType, arg1, arg2, partS, numParts, runW)
            opDict.update({seqNum: newOp})    
            seqNum += 1
    
    #TODO: persist to Operation table     
    #TODO: persist to Query_Op_Map table

    #for a,b in opDict.iteritems():
    #    print a
    #    print b.opID, b.opType, b.arg1, b.arg2, b.partitionS, b.numParts, b.runWidth

def run(configFile):
    with open(configFile, 'r') as cf:
    
        #read first 4 lines (must strip of new line character and append space character) 
        mainc = cf.readline().split(" ")[1].strip("\n") + " ";
        itr = int(cf.readline().split(" ")[1]);
        gtype = cf.readline().split(" ")[1].strip("\n");
        data = cf.readline().split(" ")[1].strip("\n") + " ";

        gtypeParam = "--type "
        dataParam = "--data "
   
        #read queries from file 
        for ln in cf:
            line = ln.split(" ");
            qname = line[0]
            query = " ".join(line[1:-1]) + " " + line[-1].strip("\n") + " "
            sbtCommand = "sbt \"run-main " + mainc + query + dataParam + data + gtypeParam + gtype + "\"";
            startType = 0 #default to cold start
        
            print "Collecting arguments passed to the query..."    
            collect_args(query);
        
            print "Running the sbt command against dataset..."
            for i in range (1, itr+1):
                #p1 = Popen(sbtCommand, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True);
                #output =  p1.stdout.read();
                print "Collecting runtime information for operations..."
                #collect_time(output);
           

if __name__ == "__main__":
    if(not len(sys.argv) > 1):
        print ("ERROR: you must pass in a temporal graph query config file to read from")
        exit();
    else:
       arg1 = sys.argv[1];
       run(arg1);
