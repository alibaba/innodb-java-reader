#


```
#!/bin/bash

start_time=$(date +%s)

java -jar /Users/xu/IdeaProjects/innodb-java-reader-ali-github/innodb-java-reader-cli/target/innodb-java-reader-cli.jar \
 -ibd-file-path /usr/local/mysql/data/test/product002.ibd \
 -create-table-sql-file-path product002.sql \
 -showheader \
 -c range-query-by-pk \
 -args 1000000,2000000 > innodb-java-reader-result.out

end_time=$(date +%s)
cost_time=$[ $end_time-$start_time ]
echo "elapsed ${cost_time}s"

md5 innodb-java-reader-result.out
```

```
#!/bin/bash

start_time=$(date +%s)

mysql -uroot -P3306 -e "select * from test.product002 where id >= 1000000 and id < 2000000" > mysql-result.out

end_time=$(date +%s)
cost_time=$[ $end_time-$start_time ]
echo "elapsed ${cost_time}s"

cat mysql-result.out | tr "\t" "," > mysql-result2.out
md5 mysql-result2.out
```