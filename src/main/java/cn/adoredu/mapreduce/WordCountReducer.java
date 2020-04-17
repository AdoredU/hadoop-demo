package cn.adoredu.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * MR程序reduce阶段的处理类：
 *
 * KEYIN：reduce阶段输入key类型，对应mapper阶段输出key类型，这里就是单词 Text；
 * VALUEIN：reduce阶段输入value类型，对应mapper阶段输出value类型，这里是单词次数， IntWritable；
 * KEYOUT：单词，Text；
 * VALUEOUT：单词总次数，IntWritable；
 *
 * Reducer生命周期方法：与Mapper中类似。
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    /**
     * reduce接收所有来自mapper阶段的数据，按照key字典序排序。然后按照key是否相同调用reduce方法。
     * 本方法把key作为参数key，所有v作为迭代器作为values。
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        // 遍历一组迭代器
        for (IntWritable value : values) {
            count += value.get();
        }
        context.write(key, new IntWritable(count));
    }
}
