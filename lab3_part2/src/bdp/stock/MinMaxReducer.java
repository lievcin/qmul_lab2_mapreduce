package bdp.stock;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;


public class MinMaxReducer extends Reducer<Text, DblDblPair, Text, Text> {

    public void reduce(Text key, Iterable<DblDblPair> year_prices, Context context)
      throws IOException, InterruptedException {

    		Text stock_summary = new Text();

      	int min_year = 0; //because we know they cannot be less than zero
       	int max_year = 0;

       	double min_price = Double.MAX_VALUE;
       	double max_price = Double.MIN_VALUE;

				for (DblDblPair year_price : year_prices) {

						// to find min-max prices of a stock
		        if (year_price.getSecond().get() > max_price ){
		        		max_price = year_price.getSecond().get();
		        }
		        if (year_price.getSecond().get() < min_price ){
		        		min_price = year_price.getSecond().get();
		        }

		    }

		   stock_summary.set("MIN: " + min_price + "   MAX:" + max_price);
		   // stock_summary.set("MIN: ");
       context.write(key, stock_summary);

    }
}
