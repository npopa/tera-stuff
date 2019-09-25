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

export TABLE='default:T'

hbase ltt -tn ${TABLE} \
          -init_only \
          -compression SNAPPY \
          -families 0,1 \
          -num_regions_per_server 5

cat<<EOF|hbase shell
#Use the below to set/unset table properties.
# MAX_FILESIZE - default 10GB
# SPLIT_POLICY - default org.apache.hadoop.hbase.regionserver.IncreasingToUpperBoundRegionSplitPolicy
#     - org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy
#     - org.apache.hadoop.hbase.regionserver.IncreasingToUpperBoundRegionSplitPolicy
#     - org.apache.hadoop.hbase.regionserver.SteppingSplitPolicy
# MEMSTORE_FLUSHSIZE - default 128MB
# COMPACTION_ENABLED
# FLUSH_POLICY
# alter '${TABLE}', MAX_FILESIZE => '100000000'
# alter '${TABLE}', METHOD => 'table_att_unset', NAME => 'MAX_FILESIZE'

alter '${TABLE}', MAX_FILESIZE => '100000000'
alter '${TABLE}', MEMSTORE_FLUSHSIZE => '20000000'
alter '${TABLE}', SPLIT_POLICY => 'org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy'
desc '${TABLE}'
EOF


hbase ltt -tn ${TABLE} \
          -skip_init \
          -families 0,1 \
          -write 5:5000 \
          -num_keys 100000

export JAR=/root/tera-stuff/target/tera-stuff.jar
export CLASSPATH=`hadoop classpath`:`hbase mapredcp`:/etc/hbase/conf:$JAR
export HADOOP_CLASSPATH=${CLASSPATH}

export FLD=$(echo ${TABLE}|sed 's/:/_/')
export KEYS_SAMPLE=/tmp/${FLD}_sample

export KEYS_RND=/tmp/${FLD}_keys_rnd
export KEYS10=/tmp/${FLD}_keys10
export KEYS_RND10=hdfs://nameservice-prod02/tmp/${TABLE}_keys_rnd10
export KEYS_SIZE=hdfs://nameservice-prod02/tmp/${TABLE}_keys_size
export OUTPUT=hdfs://nameservice-prod02/tmp/${TABLE}
export OUTPUT_RND=hdfs://nameservice-prod02/tmp/${TABLE}_random_reads
export OUTPUT_RND10=hdfs://nameservice-prod02/tmp/${TABLE}_random_reads10


kinit -kt $(ls -tr /var/run/cloudera-scm-agent/process/*/hbase.keytab|tail -1) hbase/$(hostname -f)          

## Test SCAN workload
####regular table export
hdfs dfs -rmr -skipTrash ${SCAN}
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
   -Dmapreduce.job.reduces=1 \
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
    --shuffle \
    --samplePercent 10

####export table keys and the row sizes. Generate ony one file. 
####This can be used to analyze the table later.
hdfs dfs -rmr -skipTrash ${KEYS_SIZE}
yarn jar ${JAR} \
   com.cloudera.ps.terastuff.ExportTableKeys \
   -Dhbase.client.scanner.caching=100 \
   -Dmapreduce.map.speculative=false \
   -Dmapreduce.reduce.speculative=false \
   -Dmapreduce.job.reduces=1 \
    --tableName ${TABLE} \
    --outputPath ${KEYS_SIZE} \
    --includeRowSize


####export table keys and the row sizes.
####This can be used to analyze the table later.
hdfs dfs -rmr -skipTrash ${KEYS_SAMPLE}
yarn jar ${JAR} \
   com.cloudera.ps.terastuff.SampleKeys \
   -Dhbase.client.scanner.caching=100 \
   -Dmapreduce.map.speculative=false \
   -Dmapreduce.reduce.speculative=false \
   -Dmapreduce.job.reduces=0 \
    --tableName ${TABLE} \
    --outputPath ${KEYS_SAMPLE} \
    --includeRowSize \
    --sampleCount 1000

####Export the table using gets rather than scan. Use the --keysPath as input for the keys to be exported.
hdfs dfs -rmr -skipTrash ${OUTPUT_RND}
yarn jar ${JAR} \
   com.cloudera.ps.terastuff.ExportKeys \
   --keysPath ${KEYS_RND} \
   --tableName ${TABLE} \
   --outputPath ${OUTPUT_RND} 

   