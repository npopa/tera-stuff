package com.cloudera.ps.terastuff;

import java.io.IOException;
import java.util.Random;
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
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class ExportTableKeys extends Configured implements Tool {
  private static final Log LOG = LogFactory.getLog(ExportTableKeys.class);
  private Options options = new Options();

  private String outputPath;
  private static String table_name;
  private boolean shuffle=false;
  private boolean includeLen=false;
  private boolean useCache=false;
  private String samplePercent="0";
  private String sampleCount="0";

  public static class ExportKeys1Mapper extends TableMapper<ImmutableBytesWritable, LongWritable> {
    private static LongWritable recordSize = new LongWritable(0);
    private long sp=0;
    private long sc=0;
    private long count=0;    
    private boolean includeLen=false;
    private boolean skip=false;
    private Random rand = new Random();
    public static enum Counters {
      ROWS, 
      SIZE
    }    

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String tableName = conf.get("ExportKeys.tableName");
        sp=conf.getLong("ExportKeys.samplePercent", 0);
        sc=conf.getLong("ExportKeys.sampleCount", 0);
        includeLen=conf.getBoolean("ExportKeys.includeLen", false);

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
     
    }    
    
    public void map(ImmutableBytesWritable row, Result record, Context context)
        throws IOException, InterruptedException {
      
      long size=Result.getTotalSizeOfCells(record);
      if (includeLen) {
        recordSize.set(size);
      }
      
      if (sp>0){ //skip by percentage
        if(rand.nextInt(100) > sp){
            skip=true;
            } else {
            skip=false;
            }
      } else if (sc>0){ //skip by count
        if(count%sc!=0){
            skip=true;
            } else {
            skip=false;
            }
      }
      
      if (!skip){
        context.write(row, recordSize);
        
        context.getCounter(Counters.ROWS).increment(1);
        context.getCounter(Counters.SIZE).increment(size);
      }
      
      
    }
  }
    
  public static class ExportKeys2Mapper extends TableMapper<Text, ImmutableBytesWritable> {
    private static Text key = new Text();   
    private static LongWritable recordSize = new LongWritable(0);
    private long sp=0;
    private long sc=0;
    private long count=0;
    private boolean includeLen=false;
    private boolean skip=false;
    private Random rand = new Random();
    public static enum Counters {
      ROWS, 
      SIZE
    }   
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String tableName = conf.get("ExportKeys.tableName");
        sp=conf.getLong("ExportKeys.samplePercent", 0);
        sc=conf.getLong("ExportKeys.sampleCount", 0);       
        includeLen=conf.getBoolean("ExportKeys.includeLen", false);
        

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
     
    }     
    
      public void map(ImmutableBytesWritable row, Result record, Context context)
          throws IOException, InterruptedException {
        

        
        if (sp>0){ //skip by percentage
          if(rand.nextInt(100) > sp){
              skip=true;
              } else {
              skip=false;
              }
        } else if (sc>0){ //skip by count
          if(count%sc!=0){
              skip=true;
              } else {
              skip=false;
              }
        }
            
        if (!skip){
          key.set(UUID.randomUUID().toString());
          context.write(key, row);
          
          context.getCounter(Counters.ROWS).increment(1);
        }
        
        skip=false;
        count+=1;       

      }    
  }
  
  public static class ExportKeysReducer extends Reducer<Text, ImmutableBytesWritable, ImmutableBytesWritable, LongWritable> {
    private static LongWritable recordSize = new LongWritable(0);
    public static enum Counters {
      ROWS, 
      SIZE
    } 
    public void reduce(Text key, Iterable<ImmutableBytesWritable> values, Context context)
        throws IOException, InterruptedException {

      for (ImmutableBytesWritable k : values) {
        context.write(k, recordSize);
        context.getCounter(Counters.ROWS).increment(1);
        context.getCounter(Counters.SIZE).increment(recordSize.get());        
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
    conf.set("ExportKeys.tableName", table_name);
    conf.setBoolean("ExportKeys.shuffle", shuffle);
    conf.setBoolean("ExportKeys.includeLen", includeLen);
    conf.set("ExportKeys.samplePercent", samplePercent);
    conf.set("ExportKeys.sampleCount", sampleCount);
    

    final TableName tableName = TableName.valueOf(table_name);

    Job job = Job.getInstance(conf, "Export keys from table " + table_name + " into file " + outputPath);

    Path outputDir = new Path(outputPath);
    if (outputDir.getFileSystem(getConf()).exists(outputDir)) {
      throw new IOException("Output directory " + outputDir + " already exists.");
    }
    FileOutputFormat.setOutputPath(job, outputDir);

    job.setJarByClass(ExportTableKeys.class);
    Scan scan = new Scan();
    scan.setCacheBlocks(useCache);
    
    if(!includeLen){
      scan.setFilter(new KeyOnlyFilter());
    }
    
    if (!shuffle){ //map only
      TableMapReduceUtil.initTableMapperJob(tableName, scan, ExportKeys1Mapper.class,
          ImmutableBytesWritable.class, LongWritable.class, job);
      //job.setNumReduceTasks(0); //assume this is set externally for now
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
    options.addOption("c", "cache", false, "use cache.");    
    options.addOption("p", "samplePercent", true, "export a just a sample percentage instead of all rows.");
    options.addOption("r", "sampleCount", true, "export a just a sample record every few records instead of all rows.");    

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
    
    if (cmd.hasOption("p")) {
      samplePercent = cmd.getOptionValue("p");
    } 

    if (cmd.hasOption("r")) {
      sampleCount = cmd.getOptionValue("r");
    } 
    
    if (cmd.hasOption("c")) {
      useCache=true;
    } 
    
    return true;
  }


  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new ExportTableKeys(), args);
    System.exit(exitCode);
  }
}
