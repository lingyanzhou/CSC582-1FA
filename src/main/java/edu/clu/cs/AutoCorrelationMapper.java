package edu.clu.cs;

import java.io.IOException;
import java.util.Calendar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AutoCorrelationMapper extends
		Mapper<LongWritable, TripDataTuple, Text, AutoCorrelationTuple> {
	private static final String s_configShiftAmount = "TimeShftTableMapper_timeShiftAmount";
	private static final String s_configEndTime = "TimeShftTableMapper_endTimet";
	private static final String s_configStartTime = "TimeShftTableMapper_startTime";
	public static final Text s_keyHourlyTipSum = new Text("HourlyTipSum");
	public static final Text s_keyHourlyPickupCount = new Text("HourlyPickupCount");
	private AutoCorrelationTuple m_hourlyTipSumTable = new AutoCorrelationTuple();
	private AutoCorrelationTuple m_hourlyPickupCountTable = new AutoCorrelationTuple();
	
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
		m_hourlyTipSumTable.init(startTime, endTime,shiftAmount);
		m_hourlyPickupCountTable.init(startTime, endTime,shiftAmount);
	}

	@Override
	public void cleanup(Context context) throws IOException,
			InterruptedException {

		context.write(s_keyHourlyTipSum, m_hourlyTipSumTable);
		context.write(s_keyHourlyPickupCount, m_hourlyPickupCountTable);
	}

	@Override
	public void map(LongWritable key, TripDataTuple value, Context context)
			throws IOException, InterruptedException {
		
		m_hourlyTipSumTable.putRecord(value.getPickupDatetime().getTimeInMillis(), value.getTipAmount());
		m_hourlyPickupCountTable.putRecord(value.getPickupDatetime().getTimeInMillis(), 1);

	}
}
