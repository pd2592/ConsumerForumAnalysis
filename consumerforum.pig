A = LOAD 'hdfs://localhost:9000/user/flume/consumerforum/FlumeData*' using org.apache.pig.piggybank.storage.CSVExcelStorage();

--------------------------------------
--sol1                                 
--------------------------------------

C = foreach A generate (chararray)$13 as timely_response;

D = FILTER C by timely_response == 'Yes';

E = group D by $0;

F = foreach E generate COUNT(D);

dump F;



--------------------------------------
--sol2                                 
--------------------------------------

G = foreach A generate ToDate($0, 'MM/dd/yyyy') as complaint_date, ToDate($11, 'MM/dd/yyyy') as forward_date;

I = FILTER G by (GetYear(complaint_date) == GetYear(forward_date)) AND (GetMonth(complaint_date) == GetMonth(forward_date)) AND (GetDay(complaint_date) == GetDay(forward_date));

J = group I by $0;

K = foreach J generate COUNT(I);

dump K;



--------------------------------------
--sol3                                 
--------------------------------------

L = foreach A generate (chararray)$7 as company, (chararray)$15 as complainid;

M = group L by company;

N = foreach M generate group, COUNT(l) as count;

O = order N by count desc;

P = limit O 1;

Q = filter O by count == P.count;

dump Q;




--------------------------------------
--sol4                                 
--------------------------------------

R = foreach A generate ToDate($0, 'MM/dd/yyyy') as complaint_date, (chararray)$1 as producttype, (chararray)$15 as complaintid;

r = limit R 10;

S = filter r by producttype == 'Debt collection' and GetYear(complaint_date) == 2015;

T = group S all;

U = foreach T generate COUNT(S);

dump U;
