import com.epam.training.hw3.Agg;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;

import java.util.ArrayList;

/**
 * Created by miket on 4/2/16.
 */

public class AggMRTest {
    MapDriver<Object, Text, Text, Agg.Aggs> mD;
    ReduceDriver<Text, Agg.Aggs, Text, Agg.Aggs> rD;

    @Before
    public void setUp() throws Exception {
        mD = MapDriver.newMapDriver(new Agg.Mapper());
        rD = ReduceDriver.newReduceDriver(new Agg.Reducer());
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void mapperTest() throws Exception {
        mD.withInput(new LongWritable(),
                new Text(
"b382c1c156dcbbd5b9317cb50f6a747b\t20130606000104000\tVh16OwT6OQNUXbj\tmozilla/4.0 (compatible; msie 6.0; windows nt 5.1; sv1; qqdownload 718)\t180.127.189.*\t80\t87\t1\ttFKETuqyMo1mjMp45SqfNX\t249b2c34247d400ef1cd3c6bfda4f12a\t\tmm_11402872_1272384_3182279\t300\t250\t1\t1\t0\t00fccc64a1ee2809348509b7ac2a97a5\t227\t3427\t282825712746\t0")
        ).withOutput(new Text("180.127.189.*"), new Agg.Aggs(new Text("180.127.189.*"), new IntWritable(1), new IntWritable(227))
        ).runTest();
    }

    @Test
    public void reducerTest() throws Exception {
        rD.withInput(
                new Text("127.0.0.1"),
                new ArrayList<Agg.Aggs>() {{
                    add(new Agg.Aggs(new Text("127.0.0.1"), new IntWritable(1), new IntWritable(100)));
                    add(new Agg.Aggs(new Text("127.0.0.1"), new IntWritable(1), new IntWritable(500)));
                }}
        ).withOutput(
                new Text("127.0.0.1"),
                new Agg.Aggs(new Text("127.0.0.1"), new IntWritable(2), new IntWritable(600))
        ).runTest();
    }

}
