package edu.clu.cs;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PearsonCorrelationMapper extends
		Mapper<LongWritable, TripDataTuple, Text, PearsonCorrelationTuple> {
	private static String[] s_colNames = { "passenger_count",
			"trip_time_in_secs", "trip_distance", "pickup_longitude",
			"pickup_latitude", "dropoff_longitude", "dropoff_latitude",
			"fare_amount", "surcharge", "mta_tax", "tip_amount",
			"tolls_amount", "total_amount" };
	private PearsonCorrelationTuple[] m_vals = new PearsonCorrelationTuple[78];
	private static Text[] s_keys = new Text[78];

	static {
		int ind = 0;
		for (int i = 0; i < 12; ++i) {
			for (int j = i + 1; j < 13; ++j) {
				s_keys[ind] = new Text(s_colNames[i] + "_X_" + s_colNames[j]);
				ind += 1;
			}
		}
	}

	private static int getIndex(int i, int j) {
		return j - i - 1 + (25 - i) * i / 2;
	}

	public PearsonCorrelationMapper() {
		for (int i = 0; i < 78; ++i) {
			m_vals[i] = new PearsonCorrelationTuple();
		}
	}

	@Override
	public void setup(Context context) {
		for (int i = 0; i < 78; ++i) {
			m_vals[i].clear();
		}
	}

	@Override
	public void cleanup(Context context) throws IOException,
			InterruptedException {

		for (int i = 0; i < 78; ++i) {
			context.write(s_keys[i], m_vals[i]);
		}
	}

	@Override
	public void map(LongWritable key, TripDataTuple value, Context context)
			throws IOException, InterruptedException {
		float[] vals = { value.getPassengerCount(), value.getTripTimeInSecs(),
				value.getTripDistance(), value.getPickupLongitude(),
				value.getPickupLatitude(), value.getDropoffLongitude(),
				value.getDropoffLatitude(), value.getFareAmount(),
				value.getSurcharge(), value.getMtaTax(), value.getTipAmount(),
				value.getTollsAmount(), value.getTotalAmount() };

		for (int i = 0; i < 12; ++i) {
			for (int j = i + 1; j < 13; ++j) {
				m_vals[getIndex(i, j)].putRecord(vals[i], vals[j]);
			}
		}

	}
}
