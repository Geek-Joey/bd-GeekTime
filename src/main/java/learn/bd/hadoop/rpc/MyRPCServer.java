package learn.bd.hadoop.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;

/**
 * @author Joey
 * @create 2021-07-25 21:44
 */
public class MyRPCServer {
    public static void main(String[] args) throws IOException {
        RPC.Server server = new RPC.Builder(new Configuration())
                .setProtocol(MyRPCProtocol.class)
                .setInstance(new RPCProtocolImplement())
                .setBindAddress("localhost")
                .setPort(8001).setNumHandlers(1).build();
        server.start();
    }
}
