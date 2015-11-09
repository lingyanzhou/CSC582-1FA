package edu.clu.cs;

import java.io.IOException;
import java.io.PrintWriter;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KNNTipAmountPredictorDriver {
	static public void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {

		String outPathName = "";
		String inPathName = "";
		int hour = 1;
		int dayOfWeek = 1;
		float pickupLon = Float.NaN;
		float pickupLat = Float.NaN;
		float dropoffLon = Float.NaN;
		float dropoffLat = Float.NaN;
		String venderId = "";
		String rateCode = "";
		int k = 100;

		CommandLineParser parser = new GnuParser();
		try {
			// parse the command line arguments
			CommandLine line = parser.parse(getOptions(), args);
			outPathName = line.getOptionValue("o");
			inPathName = line.getOptionValue("i");
			k = Integer.parseInt(line.getOptionValue("k", "100"));
			hour = Integer.parseInt(line.getOptionValue("H"));
			dayOfWeek = Integer.parseInt(line.getOptionValue("d"));
			pickupLon = Float.parseFloat(line.getOptionValue("x"));
			pickupLat = Float.parseFloat(line.getOptionValue("y"));
			dropoffLon = Float.parseFloat(line.getOptionValue("u"));
			dropoffLat = Float.parseFloat(line.getOptionValue("v"));
			venderId = line.getOptionValue("V");
			rateCode = line.getOptionValue("r");
			if (line.hasOption("h")) {
				printHelp();
				System.exit(0);
			}
		} catch (ParseException exp) {
			System.err.println("Parsing failed.  Reason: " + exp.getMessage());
			printHelp();
			System.exit(1);
		}
		execute(outPathName, inPathName, k, venderId, rateCode, hour,
				dayOfWeek, pickupLat, pickupLon, dropoffLat, dropoffLon);
	}

	public static void execute(String outPathName, String inPathName, int k,
			String venderId, String rateCode, int hour, int dayOfWeek,
			float pickupLat, float pickupLon, float dropoffLat, float dropoffLon)
			throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "Group3 KNNTipAmountPredictor");
		job.setJarByClass(KNNTipAmountPredictorDriver.class);
		// job.setMapperClass(WeeklyEnsembleNSHourlyBinMapper.class);

		conf = new Configuration(false);
		ChainMapper.addMapper(job, BasicTripDataFilterMapper.class,
				LongWritable.class, TripDataTuple.class, LongWritable.class,
				TripDataTuple.class, conf);

		conf = new Configuration(false);
		KNNTipAmountPredictorMapper.setTarget(conf, venderId, rateCode, hour,
				dayOfWeek, pickupLat, pickupLon, dropoffLat, dropoffLon);
		ChainMapper.addMapper(job, KNNTipAmountPredictorMapper.class,
				LongWritable.class, TripDataTuple.class, Text.class,
				FloatWritable.class, conf);

		conf = new Configuration(false);
		BottomKItemsMapper.setK(conf, k);
		ChainMapper.addMapper(job, BottomKItemsMapper.class, Text.class,
				FloatWritable.class, Text.class, FloatWritable.class,
				conf);

		conf = new Configuration(false);
		BottomKItemsReducer.setK(conf, k);
		ChainReducer.setReducer(job, BottomKItemsReducer.class,
				Text.class, FloatWritable.class, Text.class,
				FloatWritable.class, conf);

		// job.setMapOutputKeyClass(Text.class);
		// job.setMapOutputValueClass(FloatWritable.class);
		//
		// job.setOutputKeyClass(Text.class);
		// job.setOutputValueClass(NumericalSummaryTuple.class);

		job.setNumReduceTasks(1);

		job.setInputFormatClass(TripDataInputFormat.class);

		FileInputFormat.addInputPath(job, new Path(inPathName));
		FileOutputFormat.setOutputPath(job, new Path(outPathName));
		job.waitForCompletion(true);
	}

	private static void printHelp() {
		HelpFormatter formatter = new HelpFormatter();
		PrintWriter writer = new PrintWriter(System.err);
		formatter.printHelp(writer, 80,
				"hadoop jar <SomeJar> edu.clu.cs.KNNTipAmountPredictorDriver ",
				"CSC582-1 Project Help", getOptions(), 4, 8,
				"Author: Lingyan Zhou & Jianhong Zhu", true);
		writer.close();
	}

	private static Options getOptions() {
		Options options = new Options();
		options.addOption(OptionBuilder.withLongOpt("output").isRequired()
				.hasArg().withDescription("output folder").create("o"));
		options.addOption(OptionBuilder.withLongOpt("input").isRequired()
				.hasArg().withDescription("input folder").create("i"));
		options.addOption(OptionBuilder.withLongOpt("k").hasArg()
				.withDescription("k value").create("k"));
		options.addOption(OptionBuilder.withLongOpt("hour").isRequired()
				.hasArg().withDescription("hour").create("H"));
		options.addOption(OptionBuilder.withLongOpt("day_of_week").isRequired()
				.hasArg().withDescription("day of week").create("d"));
		options.addOption(OptionBuilder.withLongOpt("pickup_lat").isRequired()
				.hasArg().withDescription("pickup latitude").create("y"));
		options.addOption(OptionBuilder.withLongOpt("pickup_lon").isRequired()
				.hasArg().withDescription("pickup_longitude").create("x"));
		options.addOption(OptionBuilder.withLongOpt("dropoff_lat").isRequired()
				.hasArg().withDescription("dropoff_latitude").create("v"));
		options.addOption(OptionBuilder.withLongOpt("dropoff_lon").isRequired()
				.hasArg().withDescription("dropoff_longitude").create("u"));
		options.addOption(OptionBuilder.withLongOpt("vender_id").isRequired()
				.hasArg().withDescription("vender id").create("V"));
		options.addOption(OptionBuilder.withLongOpt("rate_code").isRequired()
				.hasArg().withDescription("rate code").create("r"));
		options.addOption(OptionBuilder.withLongOpt("help")
				.withDescription("print this help page").create("h"));
		return options;
	}
}
