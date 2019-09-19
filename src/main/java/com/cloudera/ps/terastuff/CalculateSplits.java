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
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
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
  private String families;
  private String tableName;
  private long regions=0;
  private boolean bySize=false;  
  private boolean byCount=false;   

  private void init() {

    options.addOption("k", "keysPath", true, "keysPath");
    options.addOption("c", "byCount", false, "split by record count");  
    options.addOption("z", "bySize", false, "split by size"); 
    options.addOption("r", "regions", true, "number of regions");     
    options.addOption("t", "tableName", true, "tableName");
    options.addOption("f", "families", true, "column families to create f1:f2:f3 etc.");
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
    
    if (cmd.hasOption("r")) {
      regions = Long.parseLong(cmd.getOptionValue("r"));
    }

    if (cmd.hasOption("z")) {
      bySize=true;
    }
 
    if (cmd.hasOption("c")) {
      byCount=true;
    }
    
    if (cmd.hasOption("t")) {
      tableName = cmd.getOptionValue("t");
    }

    if (cmd.hasOption("f")) {
      families = cmd.getOptionValue("f");
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
    long splitCount=(countTotal/regions)+1;
    long splitSize=(sizeTotal/regions)+1;
    long splits=0;
    LOG.info("Should have a split every:" + splitCount + " samples." );    
    Iterator<ImmutableBytesWritable> itr=sorted.keySet().iterator();
    
    byte[][] splitKeys = new byte[(int)regions][];

    long size=0;
    long count=0;
    countTotal=0;
    while(itr.hasNext())    
    {    
      ImmutableBytesWritable key=itr.next();  
      LOG.info("File:   "+hm.get(key)); 
      SequenceFile.Reader reader = null;
      try {
        reader = new SequenceFile.Reader(conf, Reader.file(new Path(keysPath+"/"+hm.get(key))), Reader.bufferSize(4096));
        ImmutableBytesWritable rowkey= new ImmutableBytesWritable();
        LongWritable value = new LongWritable();          

        while (reader.next(rowkey, value)) {
          //LOG.info("Sample: "+rowkey); 
          count+=1;
          if ((count%splitCount)==0){
            splitKeys[(int)splits]=rowkey.get();
            splits+=1;
            LOG.info("--> Split "+ String.format("%04d", splits)+" at:"+rowkey);           
          }
        }
      } finally {
        if (reader != null) {
          reader.close();
        }
      }
    }  
    LOG.info("Processed:"+ count + " sample keys.");    
    LOG.info("Total generated splits:"+ String.format("%04d", splits));
    
    Connection connection = ConnectionFactory.createConnection(conf);
    TableName tName = TableName.valueOf(tableName.getBytes());

    Admin admin = connection.getAdmin();
    
    String[] columnFamilies = families.split(":");
    HTableDescriptor desc = new HTableDescriptor(tableName);
    for (String cf : columnFamilies) {
      desc.addFamily(new HColumnDescriptor(Bytes.toBytes(cf)));
    }  
    
    admin.createTable(desc, splitKeys);
    
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
