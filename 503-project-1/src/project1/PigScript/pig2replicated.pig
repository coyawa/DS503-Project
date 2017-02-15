customers = LOAD 'input/1/customers.txt' USING PigStorage(',') AS (ID:int, Name:chararray, Age:int, CountryCode:int, Salary:float);
transactions = LOAD 'input/1/transactions.txt' USING PigStorage(',') AS (TransID:int, CustID:int, TransTotal:float, TransNumItems:int, TransDesc:chararray);

A = group transactions by CustID;
B = foreach A generate group, (int)COUNT(transactions) as NumT, SUM(transactions.TransTotal) as SumT, MIN(transactions.TransNumItems) as MinI;
C = join customers by ID, B by group using 'replicated';
RESULT = foreach C generate ID, Name, Salary, NumT, SumT, MinI;

store RESULT into 'output/Pig2_out.txt' using PigStorage(',');

