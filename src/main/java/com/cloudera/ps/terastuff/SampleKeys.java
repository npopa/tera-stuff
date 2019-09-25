package com.cloudera.ps.terastuff;

import java.io.DataInput;
import java.io.DataOutput;
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
import org.apache.hadoop.io.Writable;

public class SampleKeys extends Configured implements Tool {
  private static final Log LOG = LogFactory.getLog(SampleKeys.class);
  private Options options = new Options();

  private String outputPath;
  private static String table_name;
  private boolean includeLen=false;
  private String samplePercent="0";
  private String sampleCount="0";

  public static class SampleKeys1Mapper extends TableMapper<ImmutableBytesWritable, SampleWritable> {
    private static SampleWritable sample = new SampleWritable(0, 0);
    private static ImmutableBytesWritable rowKey  = new ImmutableBytesWritable();
    private long sp=0;
    private long sc=0;
    private long count=0;  
    private long size=0; 
    private boolean includeLen=false;
    private boolean skip=false;
    private Random rand = new Random();
    public static enum Counters {
      TOTAL_ROWS, 
      SAMPLED_ROWS,
      TOTAL_SIZE,
      SAMPLED_SIZE,     
    }    

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String tableName = conf.get("ExportKeys.tableName");
        sp=conf.getLong("ExportKeys.samplePercent", 0);
        sc=conf.getLong("ExportKeys.sampleCount", 0);
        includeLen=conf.getBoolean("ExportKeys.includeLen", false);
        
        LOG.info("ExportKeys.samplePercent="+sp);
        LOG.info("ExportKeys.sampleCount="+sc);    
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      context.getCounter(Counters.TOTAL_SIZE).increment(size);
      
      if (skip) { //write the last key/size
        if (includeLen) {
          sample.set(count, size);
        } else {
          sample.set(count, 0);
        }
        context.write(rowKey, sample);
      }
      
    }    
    
    public void map(ImmutableBytesWritable row, Result record, Context context)
        throws IOException, InterruptedException {
      
      skip=false; 
      count+=1;
      
      size+=Result.getTotalSizeOfCells(record);
      if (includeLen) {
        sample.set(count, size);
      } else {
        sample.set(count, 0);
      }
      rowKey = row;
      
      if (sp>0){ //skip by percentage
        if(rand.nextInt(100) > sp){
            skip=true;
            } else {
            skip=false;
            }
      } else if (sc>0){ //skip by count
        if((count % sc) != 1){
            skip=true;
            } else {
            skip=false;
            }
      }
      
      if (!skip){
        context.write(rowKey, sample);       
        context.getCounter(Counters.SAMPLED_ROWS).increment(1);
      }
      
    }
  }
    

  public static class SampleWritable implements Writable {
    // Some data
    private long counter;
    private long size;

    // Default constructor to allow (de)serialization
    SampleWritable() { }
    
    public SampleWritable(long counter,long size) { set(counter, size); }
    public void set(long counter,long size) { this.counter = counter; this.size = size; }

    public void write(DataOutput out) throws IOException {
      out.writeLong(counter);
      out.writeLong(size);
    }

    public void readFields(DataInput in) throws IOException {
      counter = in.readLong();
      size = in.readLong();
    }

    public static SampleWritable read(DataInput in) throws IOException {
      SampleWritable w = new SampleWritable();
      w.readFields(in);
      return w;
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

    job.setJarByClass(SampleKeys.class);
    Scan scan = new Scan();
    
    if(!includeLen){
      scan.setFilter(new KeyOnlyFilter());
    }
    
    TableMapReduceUtil.initTableMapperJob(tableName, scan, SampleKeys1Mapper.class,
          ImmutableBytesWritable.class, SampleWritable.class, job);
    job.setNumReduceTasks(0); 
    
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setOutputKeyClass(ImmutableBytesWritable.class);
    job.setOutputValueClass(SampleWritable.class);    
    return job.waitForCompletion(true) ? 0 : 1;

  }

  private void init() {

    options.addOption("o", "outputPath", true, "outputPath");
    options.addOption("t", "tableName", true, "table name ie. table1");
    options.addOption("l", "includeRowSize", false, "include row size.");   
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
    
    if (cmd.hasOption("l")) {
      includeLen=true;
    } 
    
    if (cmd.hasOption("p")) {
      samplePercent = cmd.getOptionValue("p");
    } 

    if (cmd.hasOption("r")) {
      sampleCount = cmd.getOptionValue("r");
    } 
    
    return true;
  }


  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new SampleKeys(), args);
    System.exit(exitCode);
  }
}
