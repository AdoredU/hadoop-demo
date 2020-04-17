package cn.adoredu.flowsum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    FlowBean flowBean = new FlowBean();
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long upFlowCount = 0;
        long downFlowCount = 0;

        for (FlowBean flowBean : values) {
            upFlowCount += flowBean.getUpFlow();
            downFlowCount += flowBean.getDownFlow();
        }
        flowBean.set(upFlowCount, downFlowCount);

        context.write(key, flowBean);
    }
}
