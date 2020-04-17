package cn.adoredu.flowsum;

import cn.adoredu.flowsum.partitioner.ProvincePartitioner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FlowSumDriver {

    public static void main(String[] args) throws Exception {
        // 通过Job封装本次mr执行的信息
        Configuration conf = new Configuration();
        // 是否本地执行取决于该配置信息，值为local时表示本地执行
        conf.set("mapreduce.framework.name", "local");  // 实际上该配置可以省略，因为mapred-defalut.xml中默认就是local
        Job job = Job.getInstance();

        // 指定运行主类
        job.setJarByClass(FlowSumDriver.class);

        // 指定mapper和reducer
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        // 指定mapper阶段的输出k/v类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        // 指定最终输出的k/v类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        job.setNumReduceTasks(3);
        // 设置分区规则
        job.setPartitionerClass(ProvincePartitioner.class);

        // 指定输入和输出数据位置
        // 本地执行，使用本地fs
        FileInputFormat.setInputPaths(job, "/Users/gp/Desktop/tmp/flowsum/input");
        FileOutputFormat.setOutputPath(job, new Path("/Users/gp/Desktop/tmp/flowsum/outputprovince"));

        // 提交
//        job.submit();
        boolean res = job.waitForCompletion(true);  // 使用该方法可以打印执行日志
        System.exit(res?0:1);
    }
}
