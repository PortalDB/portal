import os;
import sys;
import traceback;
import models;
import peewee;
from peewee import *;

class DBConnection():
    #database = None
    #database = MySQLDatabase("temporal", host="localhost", port=3306, user="root", passwd="hoo25");

    @classmethod
    def __init__(self, db):
        self.database = db;


    def create_query(self):
        try:
            query = models.Query(query_id=None);
            query.save();
            return query.query_id

        except Exception:
            print "Exception while trying to save to \'Query\' table:"
            print traceback.format_exc()
            sys.exit(1)

    #this function checks if an operation already exists in the table before persisting
    def check_op_exists(self, op):
        try:
            #check if an exact copy of this operation exists
            operation = models.Operation.get((models.Operation.opType == op.opType) &
                                                (models.Operation.arg1 == op.arg1) & 
                                                (models.Operation.arg2 == op.arg2) &
                                                (models.Operation.partitionS == op.partitionS) &
                                                (models.Operation.numParts == op.numParts) &
                                                (models.Operation.runWidth == op.runWidth))
            return operation
        except DoesNotExist:
            return None

        except Exception:
            print "Unknown exception while trying to check \'Operation\' table:"
            print traceback.format_exc()
            sys.exit(1)

        return None

    def persist_ops(self, op_dict):
        try:
            id_dict = {}
            with self.database.transaction():
                for seqN, operation in op_dict.iteritems():
                    #check if operation already exists
                    oldOp = self.check_op_exists(operation)
    
                    if oldOp == None:
                        #persist to Operation table and generate a new op_id
                        operation.save()
                    else:
                        print "Operation", operation, "already exists in the \'Operation\' table"
                        operation.op_id = oldOp.op_id
                    
                    id_dict.update({seqN: operation.op_id})                   
            
            return id_dict
        
        except Exception:
            print "Unknown exception while trying to save to \'Operation\' table:"
            print traceback.format_exc()                    
            sys.exit(1)
        
        return None

    def persist_query_ops(self, id_dict, qid):
        query_ops = []

        try:
            for seqN, oid in id_dict.iteritems():
                que = models.Query.get(models.Query.query_id == qid)
                op = models.Operation.get(models.Operation.op_id == oid)
            
                newQ = models.Query_Op_Map(
                                query_id = que,
                                op_id = op,
                                seqNum = seqN)
                query_ops.append(newQ)

            with self.database.transaction():
                for q in query_ops:
                    print "Query info:", q.query_id, q.op_id, q.seqNum
                    q.save(force_insert=True)

        except Exception:
            print "Exception while trying to save to \'Query_Op_Map\' table:"
            print traceback.format_exc()
            sys.exit(1)
                
    def persist_buildRef(self, buildN, revNum):
        try:
            #check if revisionRed already exists
            bld = models.Build.get(models.Build.build_num == buildN)
            ref = models.Build.get(models.Build.revisionRef == revNum)
        
        except DoesNotExist:
            print "starting to create new \'build\' instance"
            build = models.Build(
                            build_num = buildN,
                            revisionRef = revNum,
                            description = "This is build #" + str(buildN))
            build.save(force_insert=True)
            return build
        
        except Exception:
            print "Exception while trying to save to \'Build\' table"
            print traceback.format_exc()
            sys.exit(1)
        
        return None


