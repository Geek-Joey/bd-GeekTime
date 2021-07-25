package learn.bd.hadoop.rpc;

import org.apache.hadoop.ipc.VersionedProtocol;

/**
 * 定义RPC接口协议，添加自定义方法,必须继承Hadoop提供的接口VersionedProtocol
 * @author Joey
 * @create 2021-07-25 21:27
 */
public interface MyRPCProtocol extends VersionedProtocol {
    //RPC client 和 server 之间必须使用相同的版本的协议才能进行正常通信
    public static final long versionID = 1L;
    //自定义方法
    public String echo(String value) throws Exception;
    public String findName(String studentId) throws Exception;
}
