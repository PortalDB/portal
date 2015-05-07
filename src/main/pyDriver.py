import sys;
sys.path.insert(0, './python')
import re;
import traceback;
import models, connect;
import subprocess;
from peewee import *;
from subprocess import Popen, PIPE;

#database = None;
#dbconnect = None;

def collect_time(output):
    ftime = -1
    op_list = ["Aggregation", "Selection", "PageRank", "Count", "GetSnapshot"] #append to this list for new opearations
    time_dict = {}
    
    #collect final runtime
    r = re.compile('Final Runtime: (.*?)ms\n')
    m = r.search(output)

    if m:
        ftime = int(m.group(1))
        time_dict.update({0 : ftime})

    #collect times for each operation
    for op in op_list:
        searchStr = op + " Runtime: (.*?)\n";
        r = re.compile(searchStr)
        m = r.findall(output)

        if m:
            for val in m:
                res = val.split(" ")
                opTime = res[0].replace("ms", "") 
                seqNum = res[1].replace("(", "").replace(")", "") 
                time_dict.update({int(seqNum) : int(opTime)})

    return time_dict  

#helper fumction for collect_args
def get_agg_type(sem):
    if sem == "universal":
        return 0;
    return 1;   
    
#helper function for collect_args
def create_op(oT, a1, a2, pS, nP, rW):
    op = models.Operation(
        #op_id = _ _ _ (autogenerated in db)
        opType = oT,
        arg1 = a1,
        arg2 = a2,
        partitionS = pS,
        numParts = nP,
        runWidth = rW )
    return op

def collect_args(query):
    print "\nQUERY:", query
    line = query.split(" ");
    opDict = {} #dictionary of all operations in the query
    seqNum = 1
    opType = arg1 = arg2 = partS = numParts = runW = None
    addOp = False

    for i in range (0, len(line)):
        if line[i] == "--agg":
            opType = "Aggregate"
            arg1 = runW = int(line[i+1])
            arg2 = get_agg_type(line[i+2]) 
            partS = numParts = None

            if (len(line) > i+3) and (line[i+3] == "-p"):
                partS = line[i+4]
                numParts = int(line[i+5])
            addOp = True
    
        if line[i] == "--select":
            opType = "Select"
            arg1 = int(line[i+1])
            arg2 = int(line[i+2])
            partS = runW = numParts = None

            if (len(line) > i+3) and (line[i+3] == "-p"):
                runW = 1 #FIXME: what is runW for selection??
                partS = line[i+4]
                numParts = int(line[i+5])
            addOp = True

        if line[i] == "--pagerank":
            opType = "PageRank"
            arg1 = int(line[i+1])
            arg2 = 1 #FIXME: what is arg2 for pr??
            partS = runW = numParts = None

            if (len(line) > i+2) and (line[i+2] == "-p"):
                runW = 1 #FIXME: what is runW for pr??
                partS = line[i+3]
                numParts = int(line[i+4])
            addOp = True

        if line[i] == "--count":
            opType = "Count"
            arg1 = arg2 = None # no args for count
            partS = runW = numParts = None            

            if (len(line) > i+1) and (line[i+1] == "-p"):
                runW = 1 #FIXME: what is runW for count?
                partS = line[i+2]
                numParts = int(line[i+3]) 
            addOp = True
        
        if line[i] == "--getsnapshot":
            opType = "GetSnapshot"
            arg1 = int(line[i+1])
            arg2 = partS = runW = numParts = None

            if (len(line) > i+2) and (line[i+2] == "-p"):
                runW = 1  #FIXME: what is runW for gs??
                partS = line[i+3]
                numParts = int(line[i+4])
            addOp = True

        #create new operation
        if addOp == True:
            newOp = create_op(opType, arg1, arg2, partS, numParts, runW)
            opDict.update({seqNum: newOp})
            seqNum += 1        
            addOp = False        

    #for a,b in opDict.iteritems():
    #    print a, "-", b.opID, b.opType, b.arg1, b.arg2, b.partitionS, b.numParts, b.runWidth
    return opDict

def run(configFile):
    with open(configFile, 'r') as cf:
    
        #read first 4 lines (must strip of new line character and append space character) 
        mainc = cf.readline().split(" ")[1].strip("\n") + " ";
        env = cf.readline().split(" ")[1].strip("\n");
        cConf = cf.readline().split(" ")[1].strip("\n") + " ";
        buildN = int(cf.readline().split(" ")[1]);
        sType = int(cf.readline().split(" ")[1]);
        itr = int(cf.readline().split(" ")[1]);
        gType = cf.readline().split(" ")[1].strip("\n");
        data = cf.readline().split(" ")[1].strip("\n") + " ";        
        
        gtypeParam = "--type "
        dataParam = "--data "
        sparkSubmit = "$SPARK_HOME/bin/spark-submit --class "
        mesosMaster = "--master mesos://master:5050 "
        scalaJar = "target/scala-2.10/tgraph-assembly-1.0.jar "
        warm = ""
        sbtCommand = ""
 
        #run with warm start
        if sType == 1:
            warm = " --warmstart"       
        
        #get git revision number and save build information
        p1 = Popen('cat ../../.git/refs/heads/master', stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        gitRev = p1.communicate()[0];

        #read queries from file 
        for ln in cf:
            #error checking for extra line at the end of file
            if not ln:
                continue;

            line = ln.split(" ");
            qname = line[0]
            query = " ".join(line[1:-1]) + " " + line[-1].strip("\n") + " "
            classArg = query + dataParam + data + gtypeParam + gType + warm
            querySaved = False;
            qRef = None;
            op_dict = id_dict = {}
            
            if env == "local" or env == "ec2":
                sbtCommand = "sbt \"run-main " + mainc + classArg + " --env " + env + "\" | tee -a log.out";
            elif env == 'mesos':
                sbtCommand = sparkSubmit + mainc + mesosMaster + scalaJar + classArg + " --env " + env + " | tee -a log.out"     
                
                #get cluster config
                p2 = Popen('curl http://master:5050/slaves', stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
                out = p2.communicate()[0];
                slaves = out.count("hostname")
                cores = 2 #default num cores
                ram = 8 #default ram
                cConf = None

                r = re.compile('"cpus":(.*?),').search(out)
                if r:
                    cores = int(r.group(1))
                
                #set cluster config
                #FIXME: find ram of slaves
                cConf = str(slaves) + "s_" + str(cores) + "c_" + str(ram) + "g"  
            
            print "STATUS: running the sbt command against dataset and collect results..."
            for i in range (1, itr+1):
                p3 = Popen(sbtCommand, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True);
                pres = p3.communicate()
                output = pres[0]
                time_dict = collect_time(output);
                rTime = None
                print output                

                try:
                    rTime = time_dict[0] #get total runtime
                except KeyError:
                    print "ERROR: Query run did not return a final runtime. See result below:"
                    print pres[1]
                    print traceback.format_exc()
                    sys.exit(1)

                #only run this once for each query
                if querySaved == False:
                    print "STATUS: persisting query information"
                    qRef = dbconnect.persist_query() #persist to Query table
                    op_dict = collect_args(query);
                    id_dict = dbconnect.persist_ops(op_dict) #persist to Operation table
                    dbconnect.persist_query_ops(qRef, id_dict) #persist tp Query_Op_Map table            
                    querySaved = True        
 
                bRef = dbconnect.persist_buildRef(buildN, gitRev.strip("\n"))
                eRef = dbconnect.persist_exec(time_dict, qRef, gType, sType, cConf, rTime, i, bRef)
                dbconnect.persist_time_op(eRef, qRef, id_dict, time_dict) 
                print "STATUS: finished running iteration", i, "of current query.." 
        print "***  Done with executions." 

if __name__ == "__main__":
    database = models.BaseModel._meta.database
    dbconnect = connect.DBConnection(database)

    if(not len(sys.argv) > 1):
        print ("ERROR: you must pass in a temporal graph query config file to read from")
        exit();
    else:
       arg1 = sys.argv[1];
       run(arg1);
