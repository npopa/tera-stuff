package com.cloudera.ps.terastuff;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.Random;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.ResultSerialization;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.*;

/*
 


*/

public class ExportKeys extends Configured implements Tool {

	private Options options = new Options();

	private String outputPath;
	private String keysPath;
	private String tableName;
    
	public static class ExportKeysMapper extends Mapper<ImmutableBytesWritable, NullWritable, ImmutableBytesWritable, Result> {

		private NullWritable _NULL_ = NullWritable.get();
		private Text rowText = new Text();

	        private Connection connection = null;
	        private Table table = null;

	        @Override
	        protected void setup(Context context) throws IOException, InterruptedException {
	            Configuration conf = context.getConfiguration();
	            String tableName = conf.get("ExportKeys.tableName");  
	            connection = ConnectionFactory.createConnection(context.getConfiguration());
	            table = connection.getTable(TableName.valueOf(tableName));

	        }

	        @Override
	        protected void cleanup(Context context) throws IOException, InterruptedException {
	          table.close();
	        }
		
		@Override
		public void map(ImmutableBytesWritable row, NullWritable nullValue, Context context)
				throws IOException, InterruptedException {
		  
            List<Get> gets = new ArrayList<Get>();
            Get get1 = new Get(row.get());
            gets.add(get1);         
      		    
			context.write(row, table.get(get1));
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

		// this should add the hbase configuration to the classpath on the
		// mappers.

		Configuration conf = getConf();
		HBaseConfiguration.merge(conf, HBaseConfiguration.create(conf));

		// Error: java.io.IOException: Could not find a deserializer for the
		// Value class: 'org.apache.hadoop.hbase.client.Result'.
		// Please ensure that the configuration 'io.serializations' is properly
		// configured, if you're using custom serialization.
		conf.setStrings("io.serializations",
				new String[] { conf.get("io.serializations"), ResultSerialization.class.getName() });
	    conf.set("ExportKeys.tableName", tableName);
        Job job = Job.getInstance(conf);
        TableMapReduceUtil.addDependencyJars(job);

        // security stuff
        if (System.getenv("HADOOP_TOKEN_FILE_LOCATION") != null) {
            conf.set("mapreduce.job.credentials.binary", System.getenv("HADOOP_TOKEN_FILE_LOCATION"));
        }
        TableMapReduceUtil.initCredentials(job);

		Path outputDir = new Path(outputPath);
		Path inputDir = new Path(keysPath);
		if (outputDir.getFileSystem(getConf()).exists(outputDir)) {
			throw new IOException("Output directory " + outputDir + " already exists.");
		}
		job.setInputFormatClass(SequenceFileInputFormat.class);
		FileInputFormat.addInputPath(job, inputDir);
		FileOutputFormat.setOutputPath(job, outputDir);
		job.setJobName("ExportKeys");
		job.setJarByClass(ExportKeys.class);
		job.setMapperClass(ExportKeysMapper.class);
		job.setNumReduceTasks(0);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(ImmutableBytesWritable.class);
		job.setOutputValueClass(Result.class);
		return job.waitForCompletion(true) ? 0 : 1;

	}

	private void init() {

		options.addOption("o", "outputPath", true, "outputPath");
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

		if (cmd.hasOption("o")) {
			outputPath = cmd.getOptionValue("o");
		}

		if (cmd.hasOption("k")) {
			keysPath = cmd.getOptionValue("k");
		}

        if (cmd.hasOption("t")) {
          tableName = cmd.getOptionValue("t");
        }		
		return true;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new ExportKeys(), args);
		System.exit(exitCode);
	}
}