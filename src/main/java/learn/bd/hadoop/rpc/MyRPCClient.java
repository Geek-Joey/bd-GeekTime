package learn.bd.hadoop.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.net.InetSocketAddress;

/**
 * @author Joey
 * @create 2021-07-25 21:45
 */
public class MyRPCClient {
    public static void main(String[] args) throws Exception {
        MyRPCProtocol client = RPC.getProxy(MyRPCProtocol.class,
                MyRPCProtocol.versionID,
                new InetSocketAddress("localhost",8001),
                new Configuration());
        String echo = client.echo("rpc服务，请帮我计算一下：");
        System.out.println(echo);
        String name = client.findName("20210123456789");
        System.out.println("收到rpc服务的计算结果:" + name);
        // 停止客户端
        RPC.stopProxy(client);
    }
}
