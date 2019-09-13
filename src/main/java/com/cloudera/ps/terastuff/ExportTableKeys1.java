package com.cloudera.ps.terastuff;

import java.io.IOException;
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;
import org.apache.hadoop.io.NullWritable;

public class ExportTableKeys1 extends Configured implements Tool {
  private static final Log LOG = LogFactory.getLog(ExportTableKeys1.class);
  private Options options = new Options();

  private String outputPath;
  private static String table_name;

  public static class ExportKeys1Mapper extends TableMapper<ImmutableBytesWritable, NullWritable> {
    private static NullWritable nullWritable = NullWritable.get();
    
    public void map(ImmutableBytesWritable row, Result values, Context context)
        throws IOException, InterruptedException {

      context.write(row, nullWritable);
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

    job.setJarByClass(ExportTableKeys1.class);
    Scan scan = new Scan();
    scan.setCacheBlocks(false);
    scan.setFilter(new KeyOnlyFilter());
    TableMapReduceUtil.initTableMapperJob(tableName, scan, ExportKeys1Mapper.class,
        ImmutableBytesWritable.class, NullWritable.class, job);
    job.setNumReduceTasks(0);
    return job.waitForCompletion(true) ? 0 : 1;

  }

  private void init() {

    options.addOption("o", "outputPath", true, "outputPath");
    options.addOption("t", "tableName", true, "table name ie. table1");

  }

  public boolean parseOptions(String args[]) throws ParseException, IOException {
    if (args.length == 0) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("eDisco1Migration", options, true);
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
    return true;
  }


  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new ExportTableKeys1(), args);
    System.exit(exitCode);
  }
}
