package cn.adoredu.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 程序运行的主类，封装程序执行的信息
 */
public class WordCountDriver {

    public static void main(String[] args) throws Exception {
        // 通过Job封装本次mr执行的信息
        Configuration conf = new Configuration();
        // 是否本地执行取决于该配置信息，值为local时表示本地执行
        conf.set("mapreduce.framework.name", "local");  // 实际上该配置可以省略，因为mapred-defalut.xml中默认就是local
        Job job = Job.getInstance();

        // 指定运行主类
        job.setJarByClass(WordCountDriver.class);

        // 指定mapper和reducer
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 指定mapper阶段的输出k/v类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 指定最终输出的k/v类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(3);  // 设置reduceTask个数

        job.setCombinerClass(WordCountReducer.class);

        // 指定输入和输出数据位置
        // 本地执行，使用本地fs
        FileInputFormat.setInputPaths(job, "/Users/gp/Desktop/tmp/input");
        FileOutputFormat.setOutputPath(job, new Path("/Users/gp/Desktop/tmp/output"));

        // 提交
//        job.submit();
        boolean res = job.waitForCompletion(true);  // 使用该方法可以打印执行日志
        System.exit(res?0:1);
    }

}
