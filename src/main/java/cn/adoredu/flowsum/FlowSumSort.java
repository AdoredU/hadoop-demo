package cn.adoredu.flowsum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowSumSort {

    public static class FlowSumSortMapper extends Mapper<LongWritable, Text, FlowBean, Text> {
        // 提前定义，以免每次map都要新创建对象
        Text v = new Text();
        FlowBean k = new FlowBean();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\t");

            String phoneNum = fields[1];
            long upFlow = Long.parseLong(fields[fields.length - 3]);
            long downFlow = Long.parseLong(fields[fields.length - 2]);

            v.set(phoneNum);
            k.set(upFlow, downFlow);

            context.write(k, v);  // 输出时kv和上次相反
        }
    }

    public static class FlowSumSortReducer extends Reducer<FlowBean, Text, Text, FlowBean> {
        @Override
        protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(values.iterator().next(), key); // reducer只作对调后输出
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
// 通过Job封装本次mr执行的信息
        Configuration conf = new Configuration();
        // 是否本地执行取决于该配置信息，值为local时表示本地执行
        conf.set("mapreduce.framework.name", "local");  // 实际上该配置可以省略，因为mapred-defalut.xml中默认就是local
        Job job = Job.getInstance();

        // 指定运行主类
        job.setJarByClass(FlowSumSort.class);

        // 指定mapper和reducer
        job.setMapperClass(FlowSumSortMapper.class);
        job.setReducerClass(FlowSumSortReducer.class);

        // 指定mapper阶段的输出k/v类型
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        // 指定最终输出的k/v类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        // 指定输入和输出数据位置
        // 本地执行，使用本地fs
        FileInputFormat.setInputPaths(job, "/Users/gp/Desktop/tmp/flowsum/output");
        FileOutputFormat.setOutputPath(job, new Path("/Users/gp/Desktop/tmp/flowsum/outputsort"));

        // 提交
//        job.submit();
        boolean res = job.waitForCompletion(true);  // 使用该方法可以打印执行日志
        System.exit(res?0:1);
    }
}
