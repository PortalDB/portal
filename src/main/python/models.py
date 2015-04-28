from peewee import *
from peewee import drop_model_tables
from datetime import date
import datetime
import commands
import os

database = MySQLDatabase("temporal", host="localhost", port=3306, user="root", passwd="hoo25")

class BaseModel(Model):
    class Meta:
        database = database

class Operation(BaseModel):
    opID = PrimaryKeyField()
    opType = CharField()
    arg1 = IntegerField(null=True)
    arg2 = IntegerField(null=True)
    partitionS = CharField(null=True)
    numParts = IntegerField(null=True)
    runWidth = IntegerField(null=True)

class Query(BaseModel):
    queryID = PrimaryKeyField()

class Query_Op_Map(BaseModel):
    queryID = ForeignKeyField(Query)
    opID = ForeignKeyField(Operation)
    seqNum = IntegerField()

    class Meta:
        primary_key = CompositeKey('queryID','opID','seqNum')

class Build(BaseModel):
    buildNum = PrimaryKeyField()
    revisionRef = CharField()
    description = TextField(null=True)

class Execution(BaseModel):
    execID = PrimaryKeyField()
    queryID = ForeignKeyField(Query)
    startType = IntegerField()  #warm = 1, cold = 0
    clusterConfig = CharField()
    runTime = FloatField()
    started = DateTimeField(default=datetime.datetime.now)
    iterationNum = IntegerField()
    buildNum = ForeignKeyField(Build)

class Time_Per_Op(BaseModel):
    execID = ForeignKeyField(Execution)
    queryID = ForeignKeyField(Query)
    opID = ForeignKeyField(Operation)
    seqNum = IntegerField()
    runTIme = FloatField()

    class Meta:
        primary_key = CompositeKey('execID', 'queryID','opID','seqNum')

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

