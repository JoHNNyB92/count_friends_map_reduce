lines = LOAD 'friendship-20-persons.txt' AS (line:chararray);
words = FOREACH lines GENERATE FLATTEN(TOKENIZE(line)) as word;
grouped = GROUP words BY word;
fs -rmr myoutput2.txt
wordcount = FOREACH grouped GENERATE group, COUNT(words)-1;
STORE wordcount INTO 'myoutput2.txt' using PigStorage();
dump wordcount;
new_lines = LOAD 'myoutput2.txt/part-r-00000' AS (word:chararray,count:int);
dump new_lines;
sorted_data = ORDER new_lines BY count DESC;
fs -rmr fight3
STORE sorted_data INTO 'fight3' using PigStorage(',');


