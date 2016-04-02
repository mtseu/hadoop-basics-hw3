package com.epam.training.hw3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by miket on 4/1/16.
 */
public class Agg {
    public static class Aggs implements Writable {
        private Text ip;
        private IntWritable visits;
        private IntWritable price;

        public Aggs() {
            ip = new Text();
            visits = new IntWritable();
            price = new IntWritable();
        }

        public Aggs(String ip, int visits, int price) {
            this.ip = new Text(ip);
            this.visits = new IntWritable(visits);
            this.price = new IntWritable(price);
        }

        public Aggs(Text ip, IntWritable visits, IntWritable price) {
            this.ip = ip;
            this.visits = visits;
            this.price = price;
        }

        public Aggs(Text ip, int visits, int price) {
            this.ip = ip;
            this.visits = new IntWritable(visits);
            this.price = new IntWritable(price);
        }

        @Override
        public void write(DataOutput out) throws IOException {
            ip.write(out);
            visits.write(out);
            price.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            ip.readFields(in);
            visits.readFields(in);
            price.readFields(in);
        }

        @Override
        public String toString() {
            return ip + "\t" + visits + "\t" + price;
        }

        public Text getIp() {
            return ip;
        }

        public void setIp(Text ip) {
            this.ip = ip;
        }

        public IntWritable getVisits() {
            return visits;
        }

        public void setVisits(IntWritable visits) {
            this.visits = visits;
        }

        public IntWritable getPrice() {
            return price;
        }

        public void setPrice(IntWritable price) {
            this.price = price;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Aggs aggs = (Aggs) o;

            if (!ip.equals(aggs.ip)) return false;
            if (!visits.equals(aggs.visits)) return false;
            return price.equals(aggs.price);

        }

        @Override
        public int hashCode() {
            int result = ip.hashCode();
            result = 31 * result + visits.hashCode();
            result = 31 * result + price.hashCode();
            return result;
        }
    }

    public static class Mapper extends org.apache.hadoop.mapreduce.Mapper<Object,Text,Text,Aggs> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Aggs aggs = split(value);
            context.write(aggs.getIp(), aggs);
        }

        private final static Aggs split(Text line) {
            String str = line.toString();
            String[] fields = str.split("\t");

            return new Aggs(fields[4], 1, Integer.valueOf(fields[18]));
        }
    }

    public static class Reducer extends org.apache.hadoop.mapreduce.Reducer<Text,Aggs,Text,Aggs> {
        @Override
        protected void reduce(Text key, Iterable<Aggs> values, Context context) throws IOException, InterruptedException {
            int visits = 0;
            int price = 0;

            for(Aggs aggs: values) {
                visits += aggs.getVisits().get();
                price += aggs.getPrice().get();
            }

            context.write(key, new Aggs(key, visits, price));
        }
    }


    public static void main(String args[]) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "AggregationByIPMapReduceJob");
        job.setJarByClass(Agg.class);

        job.setMapperClass(Mapper.class);
        job.setCombinerClass(Reducer.class);
        job.setReducerClass(Reducer.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Aggs.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        SequenceFileOutputFormat.setOutputPath(job, new Path(args[1]));
        SequenceFileOutputFormat.setCompressOutput(job, true);
        SequenceFileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);

        boolean res = job.waitForCompletion(true);

        System.exit(res?0:1);
    }
}
