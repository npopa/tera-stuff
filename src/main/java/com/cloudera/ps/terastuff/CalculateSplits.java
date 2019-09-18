package com.cloudera.ps.terastuff;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeMap;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CalculateSplits extends Configured implements Tool {
  private static final Log LOG = LogFactory.getLog(CalculateSplits.class);
  private Options options = new Options();
  private String keysPath;
  private long splits=0;
  private boolean bySize=false;  
  private boolean byCount=false;   

  private void init() {

    options.addOption("k", "keysPath", true, "keysPath");
    options.addOption("c", "byCount", false, "split by record count");  
    options.addOption("z", "bySize", false, "split by size"); 
    options.addOption("s", "splits", true, "number of splits");     
    options.addOption("t", "tableName", true, "tableName");
  }

  public boolean parseOptions(String args[]) throws ParseException, IOException {
    if (args.length == 0) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("CalculateSplits", options, true);
      return false;
    }
    CommandLineParser parser = new PosixParser();
    CommandLine cmd = parser.parse(options, args);


    if (cmd.hasOption("k")) {
      keysPath = cmd.getOptionValue("k");
    }
    
    if (cmd.hasOption("s")) {
      splits = Long.parseLong(cmd.getOptionValue("s"));
    }

    if (cmd.hasOption("z")) {
      bySize=true;
    }
 
    if (cmd.hasOption("c")) {
      byCount=true;
    }
    
    return true;
  }


  @Override
  public int run(String[] args) throws Exception {
    String[] otherArgs = new GenericOptionsParser(getConf(), args).getRemainingArgs();

    init();
    try {
      if (!parseOptions(otherArgs))
        return 1;
    } catch (IOException ex) {

      return 1;
    }

    Configuration conf = getConf();
    HBaseConfiguration.merge(conf, HBaseConfiguration.create(conf));
    
    long sizeTotal=0;
    long countTotal=0;
    HashMap<ImmutableBytesWritable, String> hm=new HashMap<ImmutableBytesWritable, String>();  

    try {
      FileSystem fs = FileSystem.get(conf);
      LOG.info("Reading keys from "+keysPath);
      // the second boolean parameter here sets the recursion to true
      RemoteIterator<LocatedFileStatus> fileStatusListIterator =
          fs.listFiles(new Path(keysPath), false);
      while (fileStatusListIterator.hasNext()) {
        LocatedFileStatus fileStatus = fileStatusListIterator.next();

        Path inFile = fileStatus.getPath();
        if(inFile.getName().startsWith("_")) {
          continue;
        }       
        
        SequenceFile.Reader reader = null;
        try {
          ImmutableBytesWritable key = new ImmutableBytesWritable();
          LongWritable value = new LongWritable();
          reader = new SequenceFile.Reader(conf, Reader.file(inFile), Reader.bufferSize(4096));
          ImmutableBytesWritable firstKey= new ImmutableBytesWritable();
          ImmutableBytesWritable lastKey= new ImmutableBytesWritable();          
          long size=0;
          long count=0;
          while (reader.next(key, value)) {
            if(count==0){
              firstKey.set(key.get());
            }
            count+=1;
          }
          size=value.get();
          lastKey.set(key.get());
          hm.put(firstKey, inFile.getName());
          countTotal+=count;
          sizeTotal+=size;
        } finally {
          if (reader != null) {
            reader.close();
          }
        }
      }
    } catch (IOException e) {
      // TODO Auto-generated catch bloc
      e.printStackTrace();
    }
    LOG.info("Total sample keys:" + countTotal); 
    LOG.info("Total approximate size:" + sizeTotal);     
    LOG.info("Sorting based of the first key from each file.");    
    TreeMap<ImmutableBytesWritable, String> sorted = new TreeMap<>();
    sorted.putAll(hm);

    LOG.info("Computing splits:" + (countTotal/splits));    
    Iterator itr=sorted.keySet().iterator();               
    while(itr.hasNext())    
    {    
      ImmutableBytesWritable key=(ImmutableBytesWritable)itr.next();  
      System.out.println("Key: "+key+" file:   "+hm.get(key));  
    }  
    return 0;

  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new CalculateSplits(), args);
    System.exit(exitCode);
  }
  
  public class Splits{
    ImmutableBytesWritable startKey;
    ImmutableBytesWritable endKey;
    long keyCount;
    long size;
  }
  
}
