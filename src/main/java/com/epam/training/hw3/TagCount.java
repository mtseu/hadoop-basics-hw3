package com.epam.training.hw3;

import org.apache.commons.io.output.BrokenOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.net.URI;
import java.util.*;

/**
 * Created by miket on 4/2/16.
 */
public class TagCount {
    private static final IntWritable one = new IntWritable(1);
    private enum Browser {
        CHROME("chrome"),
        FIREFOX("firefox"),
        Safari("safari"),
        IE("ie"),
        OTHER("");

        private final String descriminator;

        Browser(String descriminator) {
            this.descriminator = descriminator;
        }

        String getDescriminator() {
            return descriminator;
        }

        static Browser getByUAString(String uas) {
            for(Browser b: Browser.values()) {
                if (uas.indexOf(b.getDescriminator()) != -1) {
                    return b;
                }
            }

            return OTHER;
        }
    }

    public static class Mapper extends org.apache.hadoop.mapreduce.Mapper<Object, Text, Text, IntWritable> {
        Map<Text, List<Text>> dict = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Path paths[] = context.getLocalCacheFiles();

            if(paths != null && paths.length > 0) {
                // hope nothing apart mine is in th cache
                try {
                    loadDict(paths[0]);
                } catch (Exception e) {
                    throw new IOException("Failed to load dictionary from:" + paths[0], e);
                }
            }
        }

        private void loadDict(Path f) throws IOException {
            try (BufferedReader reader = new BufferedReader(new FileReader(f.toString()))) {
                String str = reader.readLine(); // skip header

                while((str = reader.readLine()) != null) {
                    String[] fields = str.split("\t");

                    List<Text> tags = new ArrayList<>();
                    if (fields[1] != null && fields[1].length() > 0) {
                        for (String tag : fields[1].split(",")) {
                            tags.add(new Text(tag));
                        }
                    }

                    dict.put(new Text(fields[0]), tags);
                }
            }
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String str = value.toString();
            String fields[] = str.split("\t");
            Text userTagsId = new Text(fields[20]);

            List<Text> tags = dict.get(userTagsId);
            if(tags != null) {
                for(Text tag: tags) {
                    context.write(tag, one);
                }
            }

            context.getCounter(Browser.getByUAString(fields[3])).increment(1);
        }
    }

    public static class Reducer extends org.apache.hadoop.mapreduce.Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;

            for(IntWritable val: values) {
                count += val.get();
            }

            context.write(key, new IntWritable(count));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "CountTagsJob");
        job.setJarByClass(Agg.class);

        job.addCacheFile(new URI(args[2]));

        job.setMapperClass(Mapper.class);
        job.setCombinerClass(Reducer.class);
        job.setReducerClass(Reducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean res = job.waitForCompletion(true);

        for(Browser br: Browser.values()) {
            System.out.println("Count of " + br.toString() + ": " + job.getCounters().findCounter(br).getValue());
        }
    }
}