package com.cloudera.ps.terastuff;

import java.io.IOException;
import java.util.NavigableMap;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.ResultSerialization;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.*;

/*
 

export CLASSPATH=`hbase classpath`:$CLASSPATH
export CLASSPATH=`hadoop classpath`:$CLASSPATH
export HADOOP_CLASSPATH=$CLASSPATH:$HADOOP_CLASSPATH

export INPUT_PATH=/tmp/t2
export OUTPUT_PATH=/tmp/output

hdfs dfs -rm -R -skipTrash $OUTPUT_PATH 

yarn jar /home/ec2-user/hbase-samples-0.0.1-SNAPSHOT.jar \
   com.sa.npopa.samples.util.HBaseExportParser \
   --inputPath $INPUT_PATH \
   --outputPath $OUTPUT_PATH 


*/

public class ExportKeys extends Configured implements Tool {

	private Options options = new Options();

	private String outputPath;
	private String inputPath;

	public static class HBaseExportParserMapper extends Mapper<ImmutableBytesWritable, Result, Text, NullWritable> {

		private NullWritable _NULL_ = NullWritable.get();
		private Text rowText = new Text();

		@Override
		public void map(ImmutableBytesWritable row, Result value, Context context)
				throws IOException, InterruptedException {

			// you can play in various ways with the Result class.
			// Below is just an example where I assume all (CF,CQ,Values etc.)
			// are String.

			StringBuilder sb = new StringBuilder();
			sb.append(Bytes.toString(row.get()) + ",");
			NavigableMap<byte[], NavigableMap<byte[], byte[]>> fMap = value.getNoVersionMap();
			for (byte[] familyBytes : fMap.keySet()) {
				NavigableMap<byte[], byte[]> qMap = fMap.get(familyBytes);
				for (byte[] qualifier : qMap.keySet()) {
					sb.append("[" + Bytes.toString(familyBytes) + ":" + Bytes.toString(qualifier) + "="
							+ Bytes.toString(qMap.get(qualifier)) + "] ");
				}
			}
			rowText.set(sb.toString());
			context.write(rowText, _NULL_);
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

		Job job = Job.getInstance(conf);
		TableMapReduceUtil.addDependencyJars(job);

		Path outputDir = new Path(outputPath);
		Path inputDir = new Path(inputPath);
		if (outputDir.getFileSystem(getConf()).exists(outputDir)) {
			throw new IOException("Output directory " + outputDir + " already exists.");
		}
		job.setInputFormatClass(SequenceFileInputFormat.class);
		FileInputFormat.addInputPath(job, inputDir);
		FileOutputFormat.setOutputPath(job, outputDir);
		job.setJobName("HBaseExportParser");
		job.setJarByClass(ExportKeys.class);
		job.setMapperClass(HBaseExportParserMapper.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		return job.waitForCompletion(true) ? 0 : 1;

	}

	private void init() {

		options.addOption("o", "outputPath", true, "outputPath");
		options.addOption("i", "inputPath", true, "inputPath");

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

		if (cmd.hasOption("i")) {
			inputPath = cmd.getOptionValue("i");
		}

		return true;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new ExportKeys(), args);
		System.exit(exitCode);
	}
}
