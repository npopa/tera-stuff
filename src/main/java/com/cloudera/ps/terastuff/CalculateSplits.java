package com.cloudera.ps.terastuff;

import java.io.IOException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
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
  private Options options = new Options();
  private String keysPath;

  private void init() {

    options.addOption("k", "keysPath", true, "keysPath");
    options.addOption("t", "tableName", true, "tableName");
  }

  public boolean parseOptions(String args[]) throws ParseException, IOException {
    if (args.length == 0) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("HBaseExportParser", options, true);
      return false;
    }
    CommandLineParser parser = new PosixParser();
    CommandLine cmd = parser.parse(options, args);


    if (cmd.hasOption("k")) {
      keysPath = cmd.getOptionValue("k");
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
    
    long recordCount;

    try {
      FileSystem fs = FileSystem.get(conf);

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
          /*
          while (reader.next(key, value)) {
            System.out.println("Key " + key + " Value " + value);
          }*/
          reader.next(key, value);
          System.out.println("Key " + key + " Value " + value);
          
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
    return 0;

  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new CalculateSplits(), args);
    System.exit(exitCode);
  }
}
