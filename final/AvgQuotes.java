package nyu.cs9223;

import java.io.Serializable;
import java.util.Date;

public class AvgQuotes implements Serializable {
	private String symbol;
	private Double price;
	private Date eventtime;

	public static AvgQuotes newInstance(String symbol, Double price, Date eventTime) {
		AvgQuotes quotes = new AvgQuotes();
		quotes.setSymbol(symbol);
		quotes.setPrice(price);
		quotes.setEventtime(eventTime);
		return quotes;
	}

	public Date getEventtime() {
		return eventtime;
	}

	public void setEventtime(Date etime) {
		eventtime = etime;
	}

	public Double getPrice() {
		return price;
	}

	public void setPrice(Double p) {
		price = p;
	}

	public String getSymbol() {
		return symbol;
	}

	public void setSymbol(String s) {
		symbol = s;
	}
}
