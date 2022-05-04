package csc369;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.Date;
import java.util.Calendar;
import java.text.ParseException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.commons.lang3.time.DateUtils;

public class DateCount {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = IntWritable.class;

    public static class MapperImpl extends Mapper<LongWritable, Text, Text, IntWritable> {
	private final IntWritable one = new IntWritable(1);
	private Text word = new Text();

        @Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] str = value.toString().split(" ");
            try {
                Date d = DateUtils.parseDateStrictly(str[3], new String[]{"[dd/MMM/yyyy:HH:mm:ss"});
                Calendar c = Calendar.getInstance();
                c.setTime(d);
                System.out.println(Integer.toString(c.get(Calendar.YEAR)));

                word.set(Integer.toString(c.get(Calendar.YEAR)) + " " + (c.get(Calendar.MONTH) < 10 ? "0" : "") + Integer.toString(c.get(Calendar.MONTH) + 1));
                context.write(word, one);
            } catch (ParseException e) {
                System.exit(1);
            }
        }
    }

    public static class ReducerImpl extends Reducer<Text, IntWritable, Text, IntWritable> {
	private IntWritable result = new IntWritable();
    
        @Override
	protected void reduce(Text word, Iterable<IntWritable> intOne, Context context) throws IOException, InterruptedException {
            int sum = 0;
            Iterator<IntWritable> itr = intOne.iterator();
        
            // try {
            //     Date d = DateUtils.parseDate(str[3], new String[]{"[dd/MMM/YYYY:HH:mm:ss"});
            //     word.set(Integer.toString(d.getYear()) + Integer.toString(d.getMonth()));
            //     context.write(word, one);
            // } catch (ParseException e) {
            //     System.exit(1);
            // }
            while (itr.hasNext()) {
                sum  += itr.next().get();
            }
            result.set(sum);
            context.write(word, result);
       }
    }

}
