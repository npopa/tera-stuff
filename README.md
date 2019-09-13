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

hbase ltt -tn TestTable \
          -write 1:10000 \
          -num_regions_per_server 5\
          -num_keys 100000

kinit -kt $(ls -tr /var/run/cloudera-scm-agent/process/*/hbase.keytab|tail -1) hbase/$(hostname -f)          

export CLASSPATH=${CLASSPATH}:`hadoop classpath`:`hbase mapredcp`:/etc/hbase/conf
export HADOOP_CLASSPATH=${CLASSPATH}
export TABLE=TestTable
export KEYS=/tmp/${TABLE}_keys
export OUTPUT=/tmp/${TABLE}
export OUTPUT_RND=/tmp/${TABLE}_random_reads
export JAR=/root/tera-stuff/target/tera-stuff.jar

hdfs dfs -rmr -skipTrash ${OUTPUT}
hbase org.apache.hadoop.hbase.mapreduce.Export \
   -Dhbase.client.scanner.caching=100 \
   -Dmapreduce.map.speculative=false \
   -Dmapreduce.reduce.speculative=false \
   ${TABLE} \
   ${OUTPUT}

hdfs dfs -rmr -skipTrash ${KEYS}
yarn jar ${JAR} \
   com.cloudera.ps.terastuff.ExportTableKeys \
   -Dhbase.client.scanner.caching=100 \
   -Dmapreduce.map.speculative=false \
   -Dmapreduce.reduce.speculative=false \
    --tableName ${TABLE} \
    --outputPath ${KEYS}   

hdfs dfs -rmr -skipTrash ${OUTPUT_RND}
yarn jar ${JAR} \
   com.cloudera.ps.terastuff.ExportKeys \
   --keysPath ${KEYS} \
   --tableName ${TABLE} \
   --outputPath ${OUTPUT_RND} 

   