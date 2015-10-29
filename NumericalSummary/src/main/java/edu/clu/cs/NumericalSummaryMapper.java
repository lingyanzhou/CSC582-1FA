package edu.clu.cs;

import java.io.IOException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;

public class NumericalSummaryMapper extends
		Mapper<LongWritable, TripDataTuple, Text, FloatWritable> {
	private FloatWritable m_val = new FloatWritable();
	private Text m_colKey = new Text();
	private Set<String> m_colName = new HashSet<String>();

	@Override
	public void setup(Context context) {
		String[] cols = context.getConfiguration().get("cols").split(",");
		for (int i = 0; i < cols.length; ++i) {
			m_colName.add(cols[i].trim());
		}
	}

	@Override
	public void map(LongWritable key, TripDataTuple value, Context context)
			throws IOException, InterruptedException {

		for (String colName : m_colName) {
			m_colKey.set(colName);
			if (colName.equals("rate_code")) {
				m_val.set(value.getRateCode());
			} else if (colName.equals("passenger_count")) {
				m_val.set(value.getPassengerCount());
			} else if (colName.equals("trip_time_in_secs")) {
				m_val.set(value.getTripTimeInSecs());
			} else if (colName.equals("trip_distance")) {
				m_val.set(value.getTripDistance());
			} else if (colName.equals("pickup_longitude")) {
				m_val.set(value.getPickupLongitude());
			} else if (colName.equals("pickup_latitude")) {
				m_val.set(value.getPickupLatitude());
			} else if (colName.equals("dropoff_longitude")) {
				m_val.set(value.getDropoffLongitude());
			} else if (colName.equals("drop_off_latitude")) {
				m_val.set(value.getDropoffLatitude());
			} else if (colName.equals("fare_amount")) {
				m_val.set(value.getFareAmount());
			} else if (colName.equals("surcharge")) {
				m_val.set(value.getSurcharge());
			} else if (colName.equals("mta_tax")) {
				m_val.set(value.getMtaTax());
			} else if (colName.equals("tip_amount")) {
				m_val.set(value.getTipAmount());
			} else if (colName.equals("tolls_amount")) {
				m_val.set(value.getTollsAmount());
			} else if (colName.equals("total_amount")) {
				m_val.set(value.getTotalAmount());
			}

			context.write(m_colKey, m_val);
		}

	}
}
