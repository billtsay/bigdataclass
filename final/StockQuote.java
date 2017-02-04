package nyu.cs9223;

import java.io.Serializable;
import java.text.ParseException;
import java.util.Date;

import org.apache.commons.lang3.time.DateUtils;

public class StockQuote implements Serializable {
	private static final long serialVersionUID = 1L;
	private String t;
	private String id;
	private Double l;
	private String lt_dts;

	public Date getDate() throws ParseException {
		return DateUtils.parseDate(lt_dts, "yyyy-MM-dd'T'HH:mm:SS'Z'");
	}
	
	public String getStrDate() {
		return lt_dts;
	}

	public void setStrDate(String dts) {
		lt_dts = dts;
	}
	
	public Double getPrice() {
		return l;
	}

	public void setPrice(Double p) {
		l = p;
	}
	public String getSymbol() {
		return t;
	}
	
	public void setSymbol(String s) {
		t = s;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}
}
