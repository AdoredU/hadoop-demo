package cn.adoredu.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * MR程序mapper阶段的处理类：
 *
 * Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
 * KEYIN：mapper数据输入的key类型，默认的数据读取组件会一行一行的读取数据，
 *       读取一行返回一行。这里表示每一行的起始偏移量，数据类型为Long；
 * VALUEIN：这里表示读取没一行的内容，数据类型为String；
 * KEYOUT：本地输出的是单词，数据类型为String；
 * VALUEOUT：本地为单词次数，数据类型为Integer；
 *
 * 注意，这里的数据类型都为jdk的自带类型，跨网络传输序列化时效率地下，因此hadoop封装了对应的数据类型：
 * Long -> LongWritable;
 * String -> Text;
 * Integer -> Intwritable;
 * null -> NullWritable
 *
 * Mapper生命周期方法：
 * setup(): Called once at the beginning of the task
 * cleanup(): Called once at the end of the task
 * setup(): Called once for each key/value pair in the input split.
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    /**
     * mapper阶段的业务逻辑实现方法。该方法的调用取决于读取数据组件有无给mr传入数据。
     * 如果有，每传入一个kv对都调用一次。
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 拿到传入的一行内容，转化为String
        String line = value.toString();
        // 按照分隔符切割成单词数组
        String[] words = line.split(" ");
        // 遍历数组，每个单词标记1（整理交由reduce阶段）
        for (String word : words) {
            // 使用mr程序上下问context
            // 把mapper阶段处理的数据发送作为reduce阶段的输入
            context.write(new Text(word), new IntWritable(1));
        }
    }
}
