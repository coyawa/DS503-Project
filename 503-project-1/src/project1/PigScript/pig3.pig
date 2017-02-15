customers = LOAD 'input/1/customers.txt' USING PigStorage(',') AS (ID:int, Name:chararray, Age:int, CountryCode:int, Salary:float);
transactions = LOAD 'input/1/transactions.txt' USING PigStorage(',') AS (TransID:int, CustID:int, TransTotal:float, TransNumItems:int, TransDesc:chararray);

A = group customers by CountryCode;
B = foreach A generate group, COUNT(customers) as Cnum;
C = filter B by (Cnum>5000) or (Cnum<200);
RESULT = foreach C generate group;

store RESULT into 'output/Pig3_out.txt' using PigStorage(',');


