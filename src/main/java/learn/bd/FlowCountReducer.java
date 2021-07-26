package learn.bd;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Joey
 * @create 2021-07-18 22:34
 */
public class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long upFlowSum = 0;
        long downFlowSum = 0;

        // 1.将上行流量，下行流量累加
        for (FlowBean flowBean : values) {
            upFlowSum += flowBean.getUpFlow();
            downFlowSum += flowBean.getDownFlow();
        }

        // 2.封装对象
        FlowBean resultBean = new FlowBean(upFlowSum, downFlowSum);

        // 3.reduce
        context.write(key, resultBean);
    }
}
