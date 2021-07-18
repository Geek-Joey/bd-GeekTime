package learn.bd;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Joey
 * @create 2021-07-18 22:29
 */
public class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    Text k = new Text();
    FlowBean v = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 1.获取一行
        String line = value.toString();
        // 2.切割字段
        String[] fields = line.split("\t");
        // 3.封装对象,取出手机号码
        String phoneNum = fields[1];
        // 4.取出上行流量和下行流量
        long upFlow = Long.parseLong(fields[fields.length - 3]);
        long downFlow = Long.parseLong(fields[fields.length - 2]);

        k.set(phoneNum);
        v.set(downFlow, upFlow);

        // 5. map输出
        context.write(k, v);
    }
}
