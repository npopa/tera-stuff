## This is an attempt to write few versions of teragen


time yarn jar \
/var/lib/hadoop-httpfs/tera-stuff/target/tera-stuff.jar \
com.cloudera.ps.terastuff.TeraGenZero -Ddfs.replication=3 \
-Dmapreduce.job.maps=23 10b \
/tmp/teragen-data
