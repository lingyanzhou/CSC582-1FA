package edu.clu.cs;

public class NumericalValue {
	private float m_max=0;
	private float m_min=0;
	
	public NumericalValue(float min, float max) {
		m_max = max;
		m_min = min;
	} 
	
	public float convertToFloat(String str) {
		try {
			float ret = Float.parseFloat(str);
			if (ret>m_max || ret < m_min) {
				return Float.NaN;
			}
			return ret;
		} catch (NumberFormatException e) {
			return Float.NaN;
		}
	}
	
	public float getMin() {
		return m_min;
	}
	
	public float getMax() {
		return m_max;
	}

}
