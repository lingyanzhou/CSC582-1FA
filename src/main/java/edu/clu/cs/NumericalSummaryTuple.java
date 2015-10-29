package edu.clu.cs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class NumericalSummaryTuple implements Writable {
	    private float m_min = Float.NaN;
	    private float m_max = Float.NaN;
	    private float m_mean =Float.NaN;
	    private float m_stdDev = Float.NaN;
	    private long m_missingCount = 0;
	    private long m_totalCount = 0;
	    
	    public void setMin(float val) {
	    	m_min = val;
	    }
	    
	    public void setMax(float val) {
	    	m_max = val;
	    }
	    
	    public void setMean(float val) {
	    	m_mean = val;
	    }
	    
	    public void setStdDev(float val) {
	    	m_stdDev = val;
	    }
	    
	    public void setTotalCount(long val) {
	    	m_totalCount = val;
	    }
	    
	    public void setMissingCount(long val) {
	    	m_missingCount = val;
	    }
	    
	    public float getMin() {
	    	return m_min;
	    }
	    
	    public float getMax() {
	    	return m_max;
	    }
	    
	    public float getMean() {
	    	return m_mean;
	    }
	    
	    public float getStdDev() {
	    	return m_stdDev;
	    }
	    
	    public long getTotalCount() {
	    	return m_totalCount;
	    }
	    
	    public long getMissingCount() {
	    	return m_missingCount;
	    }

	    public String toString() {
	    	return "" + m_min + "\t" + m_max + "\t" + m_mean + "\t"  + m_stdDev + "\t" + m_missingCount + "\t" + m_totalCount;
	    }
	    
		public void readFields(DataInput in) throws IOException {
			m_min = in.readFloat();
		    m_max = in.readFloat();
		    m_mean = in.readFloat();
		    m_stdDev = in.readFloat();
		    m_missingCount = in.readLong();
		    m_totalCount = in.readLong();
		}
		public void write(DataOutput out) throws IOException {
			out.writeFloat(m_min);
		    out.writeFloat(m_max);
		    out.writeFloat(m_mean);
		    out.writeFloat(m_stdDev);
		    out.writeLong(m_missingCount);
		    out.writeLong(m_totalCount);
		}
	
	} 
