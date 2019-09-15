package com.cloudera.ps.terastuff;

import java.io.IOException;
import java.util.UUID;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

public class ExportTableKeys extends Configured implements Tool {
  private static final Log LOG = LogFactory.getLog(ExportTableKeys.class);
  private Options options = new Options();

  private String outputPath;
  private static String table_name;
  private boolean shuffle=false;
  private boolean includeLen=false;  

  public static class ExportKeys1Mapper extends TableMapper<ImmutableBytesWritable, LongWritable> {
    private static LongWritable recordSize = new LongWritable(0);

    
    public void map(ImmutableBytesWritable row, Result record, Context context)
        throws IOException, InterruptedException {
      recordSize.set(Result.getTotalSizeOfCells(record));
      context.write(row, recordSize);
    }
  }
    
  public static class ExportKeys2Mapper extends TableMapper<Text, ImmutableBytesWritable> {
    private static Text key = new Text();   
      public void map(ImmutableBytesWritable row, Result record, Context context)
          throws IOException, InterruptedException {
        
        key.set(UUID.randomUUID().toString());
        context.write(key, row);
      }    
  }
  
  public static class ExportKeysReducer extends Reducer<Text, ImmutableBytesWritable, ImmutableBytesWritable, LongWritable> {
    private static LongWritable recordSize = new LongWritable(0);

    public void reduce(Text key, Iterable<ImmutableBytesWritable> values, Context context)
        throws IOException, InterruptedException {

      for (ImmutableBytesWritable k : values) {
        context.write(k, recordSize);
      }
    }
  }


  @Override
  public int run(String[] args) throws Exception {

    init();

    try {
      if (!parseOptions(args))
        return 1;
    } catch (IOException ex) {

      return 1;
    }
    Configuration conf = getConf();
    HBaseConfiguration.merge(conf, HBaseConfiguration.create(conf));

    final TableName tableName = TableName.valueOf(table_name);

    Job job = Job.getInstance(conf, "Export from table " + table_name + " into file " + outputPath);

    Path outputDir = new Path(outputPath);
    if (outputDir.getFileSystem(getConf()).exists(outputDir)) {
      throw new IOException("Output directory " + outputDir + " already exists.");
    }
    FileOutputFormat.setOutputPath(job, outputDir);

    job.setJarByClass(ExportTableKeys.class);
    Scan scan = new Scan();
    scan.setCacheBlocks(false);
    if(!includeLen){
      scan.setFilter(new KeyOnlyFilter());
    }
    
    if (!shuffle){ //map only
      TableMapReduceUtil.initTableMapperJob(tableName, scan, ExportKeys1Mapper.class,
          ImmutableBytesWritable.class, NullWritable.class, job);
      //job.setNumReduceTasks(0); //assume this is set externally
    } else { //use reducers
      TableMapReduceUtil.initTableMapperJob(tableName, scan, ExportKeys2Mapper.class,
          Text.class, ImmutableBytesWritable.class, job); 
      job.setReducerClass(ExportKeysReducer.class);
    }
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setOutputKeyClass(ImmutableBytesWritable.class);
    job.setOutputValueClass(LongWritable.class);    
    return job.waitForCompletion(true) ? 0 : 1;

  }

  private void init() {

    options.addOption("o", "outputPath", true, "outputPath");
    options.addOption("t", "tableName", true, "table name ie. table1");
    options.addOption("s", "shuffle", false, "shuffle");
    options.addOption("l", "includeRowSize", false, "include row size."); 

  }

  public boolean parseOptions(String args[]) throws ParseException, IOException {
    if (args.length == 0) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("ExportTableKeys", options, true);
      return false;
    }
    CommandLineParser parser = new PosixParser();
    CommandLine cmd = parser.parse(options, args);

    if (cmd.hasOption("o")) {
      outputPath = cmd.getOptionValue("o");
    }

    if (cmd.hasOption("t")) {
      table_name = cmd.getOptionValue("t");
    }
    
    if (cmd.hasOption("s")) {
      shuffle=true;
    } 
    
    if (cmd.hasOption("l")) {
      includeLen=true;
    }    
    
    return true;
  }


  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new ExportTableKeys(), args);
    System.exit(exitCode);
  }
}
