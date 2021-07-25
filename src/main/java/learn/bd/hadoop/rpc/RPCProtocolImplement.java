package learn.bd.hadoop.rpc;
import org.apache.hadoop.ipc.ProtocolSignature;

import java.io.IOException;

/**
 * @author Joey
 * @create 2021-07-25 21:29
 */
public class RPCProtocolImplement implements MyRPCProtocol {

    @Override
    public String echo(String value) throws Exception {
        System.out.println("好的，我已收到你的信息");
        return value;
    }

    @Override
    public String findName(String studentId) throws Exception {
        System.out.println("正在帮您计算，稍等片刻……");
        String result = "";
        if (studentId.equals("20210123456789")){
            result = "心心";
        }else {
            result="输入学号有错误";
        }
        return result;
    }

    @Override
    public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
        System.out.println("getProtocolVersion被调用,protocol=" + protocol + "\t clientVersion=" + clientVersion);
        return MyRPCProtocol.versionID;
    }

    @Override
    public ProtocolSignature getProtocolSignature(String protocol, long clientVersion, int clientMethodsHash) throws IOException {
        System.out.println("===getProtocolSignature被调用===protocol=" + protocol + ",clientVersion=" + clientVersion + ",clientMethodsHash=" + clientMethodsHash);
        return new ProtocolSignature(MyRPCProtocol.versionID,null);
    }
}
