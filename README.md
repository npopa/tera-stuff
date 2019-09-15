## This is an attempt to write few versions of teragen


time yarn jar \
/var/lib/hadoop-httpfs/tera-stuff/target/tera-stuff.jar \
com.cloudera.ps.terastuff.TeraGenZero -Ddfs.replication=3 \
-Dmapreduce.job.maps=23 10b \
/tmp/teragen-data


# Create home dir for hbase
kinit -kt $(ls -tr /var/run/cloudera-scm-agent/process/*/hdfs.keytab|tail -1) hdfs/$(hostname -f)
hdfs dfs -mkdir /user/hbase
hdfs dfs -chown hbase:hbase /user/hbase
hdfs dfs -chmod 770 /user/hbase

kinit -kt $(ls -tr /var/run/cloudera-scm-agent/process/*/hbase.keytab|tail -1) hbase/$(hostname -f)  
hbase ltt -tn TestTable \
          -write 5:10000 \
          -num_regions_per_server 5\
          -num_keys 100000


export CLASSPATH=${CLASSPATH}:`hadoop classpath`:`hbase mapredcp`:/etc/hbase/conf
export HADOOP_CLASSPATH=${CLASSPATH}
export TABLE=TestTable
export KEYS=/tmp/${TABLE}_keys
export KEYS_RND=/tmp/${TABLE}_keys_rnd
export KEYS_SIZE=/tmp/${TABLE}_keys_size
export OUTPUT=/tmp/${TABLE}
export OUTPUT_RND=/tmp/${TABLE}_random_reads
export JAR=/root/tera-stuff/target/tera-stuff.jar
kinit -kt $(ls -tr /var/run/cloudera-scm-agent/process/*/hbase.keytab|tail -1) hbase/$(hostname -f)          


####regular table export
hdfs dfs -rmr -skipTrash ${OUTPUT}
hbase org.apache.hadoop.hbase.mapreduce.Export \
   -Dhbase.client.scanner.caching=100 \
   -Dmapreduce.map.speculative=false \
   -Dmapreduce.reduce.speculative=false \
   ${TABLE} \
   ${OUTPUT}

####export just table keys
hdfs dfs -rmr -skipTrash ${KEYS}
yarn jar ${JAR} \
   com.cloudera.ps.terastuff.ExportTableKeys \
   -Dhbase.client.scanner.caching=100 \
   -Dmapreduce.map.speculative=false \
   -Dmapreduce.reduce.speculative=false \
   -Dmapreduce.job.reduces=0 \
    --tableName ${TABLE} \
    --outputPath ${KEYS}   

####export just table keys. Use reducers to randomize the order of the exported keys
hdfs dfs -rmr -skipTrash ${KEYS_RND}
yarn jar ${JAR} \
   com.cloudera.ps.terastuff.ExportTableKeys \
   -Dhbase.client.scanner.caching=100 \
   -Dmapreduce.map.speculative=false \
   -Dmapreduce.reduce.speculative=false \
   -Dmapreduce.job.reduces=10 \
    --tableName ${TABLE} \
    --outputPath ${KEYS_RND} \
    --shuffle

####export table keys and the row sizes. Generate ony one file. 
####This can be used to analyze the table later.
hdfs dfs -rmr -skipTrash ${KEYS}
yarn jar ${JAR} \
   com.cloudera.ps.terastuff.ExportTableKeys \
   -Dhbase.client.scanner.caching=100 \
   -Dmapreduce.map.speculative=false \
   -Dmapreduce.reduce.speculative=false \
   -Dmapreduce.job.reduces=1 \
    --tableName ${TABLE} \
    --outputPath ${KEYS_SIZE} \
    --includeRowSize

####Export the table using gets rather than scan. Use the --keysPath as input for the keys to be exported.
hdfs dfs -rmr -skipTrash ${OUTPUT_RND}
yarn jar ${JAR} \
   com.cloudera.ps.terastuff.ExportKeys \
   --keysPath ${KEYS_RND} \
   --tableName ${TABLE} \
   --outputPath ${OUTPUT_RND} 

   