from peewee import *
from peewee import drop_model_tables
import datetime

database = MySQLDatabase("temporal", host="localhost", port=3306, user="graphxt", passwd="ilovedb")

class BaseModel(Model):
    class Meta:
        database = database

class Operation(BaseModel):
    op_id = PrimaryKeyField()
    opType = CharField()
    arg1 = CharField(null=True)
    arg2 = CharField(null=True)
    arg3 = CharField(null=True)
    arg4 = CharField(null=True)
    arg5 = CharField(null=True)
    partitionS = CharField(null=True)
    runWidth = IntegerField(null=True)

    class Meta:
        db_table = 'operation'

class Query(BaseModel):
    query_id = PrimaryKeyField()
        
    class Meta:
        db_table = 'query'

class Query_Op_Map(BaseModel):
    query_id = ForeignKeyField(Query, to_field="query_id", db_column="query_id")
    op_id = ForeignKeyField(Operation, to_field="op_id", db_column="op_id")
    seqNum = IntegerField()

    class Meta:
        primary_key = CompositeKey('query_id','op_id','seqNum')
        db_table = 'query_op_map'

class Build(BaseModel):
    build_num = IntegerField()
    revisionRef = CharField()
    description = TextField(null=True)

    class Meta:
        primary_key = CompositeKey('build_num','revisionRef')
        db_table = 'build'

class Execution(BaseModel):
    exec_id = PrimaryKeyField()
    query_id = ForeignKeyField(Query, to_field="query_id", db_column="query_id")
    graphType = CharField()
    startType = IntegerField()  #warm = 1, cold = 0
    clusterConfig = CharField()
    runTime = FloatField()
    started = DateTimeField(default=datetime.datetime.now())
    iterationNum = IntegerField()
    build_num = ForeignKeyField(Build, to_field="build_num", db_column="build_num")
    dataset = CharField()

    class Meta:
        db_table = 'execution'

class Time_Per_Op(BaseModel):
    exec_id = ForeignKeyField(Execution, to_field="exec_id", db_column="exec_id")
    query_id = ForeignKeyField(Query, to_field="query_id", db_column="query_id")
    op_id = ForeignKeyField(Operation, to_field="op_id", db_column="op_id")
    seqNum = IntegerField()
    runTime = FloatField()

    class Meta:
        primary_key = CompositeKey('exec_id', 'query_id','op_id','seqNum')
        db_table = 'time_per_op'

def setupDatabase(clearDatabase):
    if clearDatabase:
        drop_model_tables([Operation, Query, Query_Op_Map, Build, Execution, Time_Per_Op])

    if not Operation.table_exists():
        Operation.create_table()
    if not Query.table_exists():
        Query.create_table()
    if not Query_Op_Map.table_exists():
        Query_Op_Map.create_table()
    if not Build.table_exists():
        Build.create_table()
    if not Execution.table_exists():
        Execution.create_table()
    if not Time_Per_Op.table_exists():
        Time_Per_Op.create_table()

if __name__ == "__main__":
    setupDatabase(True)
    database.connect()
    exit()

