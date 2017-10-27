	package bdp.stock;

	import java.io.IOException;
	import java.util.Calendar;
	import org.apache.hadoop.io.IntWritable;
	import org.apache.hadoop.io.Text;
	import org.apache.hadoop.io.LongWritable;
	import org.apache.hadoop.io.DoubleWritable;
	import org.apache.hadoop.mapreduce.Mapper;

	public class MinMaxMapper extends Mapper<Object, DailyStock, Text, DblDblPair> {

    	private DailyStock stock = new DailyStock();
      // private Text company = new Text();
      // private DoubleWritable price = new DoubleWritable();

	    public void map(Object key, DailyStock entry, Context context) throws IOException, InterruptedException {

	    	Calendar c = Calendar.getInstance();

	    	Text company = entry.getCompany();

	    	//this is a whole mindfuck since the dates were converted to milliseconds in Long type, so having to
	    	//reverse engineer the date and get the year as an Integer.
	    	Long milliseconds = entry.getDay().get();
	    	c.setTimeInMillis(milliseconds);
	    	int year = c.get(Calendar.YEAR);

	    	DblDblPair year_price = new DblDblPair(year, entry.getClose().get());
	    	// DblDblPair year_price = new DblDblPair(1, 1);
	    	// price = entry.getClose();
		  	context.write(company, year_price);
	    }
	}
