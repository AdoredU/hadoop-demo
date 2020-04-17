package cn.adoredu.flowsum.partitioner;

import cn.adoredu.flowsum.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

public class ProvincePartitioner extends Partitioner<Text, FlowBean> {

    public static HashMap<String, Integer> provinceMap = new HashMap<String, Integer>();

    // 模拟省份划分规则
    static {
        provinceMap.put("131", 0);
        provinceMap.put("132", 1);
    }

    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {

        Integer code = provinceMap.get(text.toString().substring(0, 3));

        if (code != null) {
            return code;
        }
        return 2;
    }
}
