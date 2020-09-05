package proj.logAnalyze.cdh.logClean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class logClean {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
//        args = new String[] {"C:\\Users\\newwr\\Desktop\\access_2018_05_31.log", "C:\\Users\\newwr\\Desktop\\logCleaned"};
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //job信息
        job.setJarByClass(logClean.class);
        //map信息
        job.setMapperClass(logMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        //reduce信息
        job.setReducerClass(logReduce.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        //写进
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        //写出
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //运行job任务
        System.exit(job.waitForCompletion(true)? 0:1);

    }
}
