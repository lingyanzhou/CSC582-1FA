package edu.clu.cs;

import java.io.IOException;
import java.util.Calendar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class AutoCorrelationReducer extends
		Reducer<Text, AutoCorrelationTuple, Text, AutoCorrelationTuple> {
	private AutoCorrelationTuple m_result = new AutoCorrelationTuple();
	private MultipleOutputs<Text, AutoCorrelationTuple> m_out;
	
	private static final String s_configShiftAmount = "TimeShftTableMapper_timeShiftAmount";
	private static final String s_configEndTime = "TimeShftTableMapper_endTimet";
	private static final String s_configStartTime = "TimeShftTableMapper_startTime";
	
	public static void setTimeShiftAmount(Configuration conf, int amount) {
		conf.setInt(s_configShiftAmount, amount); 
	}
	
	public static void setTimeInterval(Configuration conf, Calendar startDate, Calendar endDate) {
		conf.setLong(s_configStartTime, startDate.getTimeInMillis()); 
		conf.setLong(s_configEndTime, endDate.getTimeInMillis()); 
	}


	@Override
	public void setup(Context context) {
		int shiftAmount = context.getConfiguration().getInt(s_configShiftAmount, 30*24);
		long startTime = context.getConfiguration().getLong(s_configStartTime, TripDataFormatHelper.s_minDate.getTimeInMillis());
		long endTime = context.getConfiguration().getLong(s_configEndTime, TripDataFormatHelper.s_maxDate.getTimeInMillis());
		m_result.init(startTime, endTime,shiftAmount);
		m_out = new MultipleOutputs<Text, AutoCorrelationTuple>(context);
	}
	
	@Override
	public void reduce(Text key, Iterable<AutoCorrelationTuple> values, Context context)
			throws IOException, InterruptedException {
		m_result.clear();
		for (AutoCorrelationTuple partial : values) {
			m_result.putTable(partial);
		}
		//context.write(key, m_result);
		m_out.write(key.toString(), key, m_result, key.toString()+"/"+key.toString());
	}
}
