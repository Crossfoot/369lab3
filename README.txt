Samuel Gross
Map Reduce implementations (numbers correspond to prompt)
1. in mapper I emitted pairs of <URL, 1>, in reducer I summed the values
2. in mapper I emitted pairs of <RespCode, 1>, in reducer I summed the values
3. in mapper I hardcoded a filter for a specific hostname and emitted pairs of <hostname, bytes>, in reducer I summed the values
4. in mapper I hardcoded a filter for a specific URL and emitted pairs of <hostname, 1>, in reducer I summed the values
    After the first job, I used the output of it to input to a second job that uses the request count as the key to sort the output
5. in mapper I parsed the date, emitting pairs of <"YYYY MM", 1>, in reducer I summed the values
6. in mapper I parsed the date, emitting pairs of <"dayofweek", bytes> for each line, in reducer I summed the values.
    After the first job, I used the output of it to input to a second job that uses the bytes as the key to sort the output

if running jobs 4 or 6, make sure both output directory and folder "tempout" are deleted.