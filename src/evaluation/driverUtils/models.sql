drop table time_per_op;
drop table execution;
drop table build;
drop table query_op_map;
drop table query;
drop table operation;

create table operation (
    op_id int primary key auto_increment,
    opType varchar(40) not null,
    arg1 int,
    arg2 int,
    partitionS varchar(40),
    numParts int,
    runWidth int
);

create table query (
    query_id int primary key auto_increment
);

create table query_op_map (
    query_id int,
    op_id int,
    seqNum int,
    primary key (query_id, op_id, seqNum),
    foreign key (query_id) references query (query_id),
    foreign key (op_id) references operation (op_id)
);

create table build (
    build_num int,
    revisionRef varchar(40),
    description longtext,
    primary key (build_num, revisionRef)
);

create table execution (
    exec_id int primary key auto_increment,
    query_id int not null,
    graphType varchar(10) not null,
    startType int not null, /* warm = 1, cold = 0 */
    clusterConfig varchar(40) not null,
    runTime float not null,
    started datetime not null,
    iterationNum int not null,
    build_num int not null,
    dataset varchar(32) not null,
    foreign key (query_id) references query (query_id),
    foreign key (build_num) references build (build_num)
);

create table time_per_op (
    exec_id int,
    query_id int,
    op_id int,
    seqNum int,
    runTime float not null,
    primary key (exec_id, query_id, op_id, seqNum),
    foreign key (exec_id) references execution (exec_id),
    foreign key (query_id) references query (query_id),
    foreign key (op_id) references operation (op_id)
);
