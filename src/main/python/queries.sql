/*average runtime for query for execution*/
create view vwAvgQueryExecTime as
    select query_id, startytpe, clusterconfig, build_num, avg(runtime)
    from execution
    group by query_id, startytpe, clusterconfig, build_num

/*average run for operation*/
create view vwAvgOpRuntime as
    select op_id, avg(runTime)
    from operation
    group by op_id

/***************************** Queries *******************************/

/*get average runtime of all queries over all of their iterations*/
select query_id, graphType, startType, clusterConfig, build_num, avg(runTime)
from execution
group by query_id

/*get average runtime of a specific query execution over all of its iterations*/
select query_id, graphType, startType, clusterConfig, build_num, avg(runTime)
from execution
where query_id = 1
group by query_id

/*get average query runtime by graphType*/
select query_id, graphType, startType, clusterConfig, build_num, avg(runTime)
from execution
group by query_id, graphType

/* compare average runtimes for a particular operation that differs by args passed*/
select o.op_id, o.opType, avg(t.runTime) as avg_runtime, o.arg1,
o.arg2, o.partitionS, o.numParts, o.runWidth
from (  select *
        from operation
        where opType = "Select") as o, time_per_op t
where o.op_id = t.op_id
group by o.op_id, o.opType, o.arg1, o.arg2, o.partitionS, o.numParts, o.runWidth;

/*compare average runtime of all operations of the same type (opType, arg1, arg2, runwidth, partitionStrategy) that differ by partitionStrategy */
select o.opType, o.arg1, o.arg2, o.partitionS, avg(t.runTime)
from operation o, time_per_op t
where o.op_id = t.op_id
and opType = "Select"
group by o.opType, o.arg1, o.arg2, o.partitionS;




