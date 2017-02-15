customers = LOAD 'input/1/customers.txt' USING PigStorage(',') AS (ID:int, Name:chararray, Age:int, CountryCode:int, Salary:float);
transactions = LOAD 'input/1/transactions.txt' USING PigStorage(',') AS (TransID:int, CustID:int, TransTotal:float, TransNumItems:int, TransDesc:chararray);

A = group transactions by CustID;
B = foreach A generate group, (int)COUNT(transactions) as Tnum;
C = join customers by ID, B by group; GroupC = group C all; Min = foreach GroupC generate MIN(C.Tnum) as MTnum;
D = filter C by Tnum == Min.MTnum;
RESULT = foreach D generate Name, Tnum;

store RESULT into 'output/Pig1_out.txt' using PigStorage(',');


