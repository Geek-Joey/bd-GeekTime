package learn.bd.hadoop.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;

/**
 * 创建一个HBase连接单例
 * @author Joey
 * @create 2021-08-01 22:04
 */
public class HBaseConn {
    private static final HBaseConn instance = new HBaseConn();
    private static Configuration config;
    private static Connection conn;

    private HBaseConn() {
        if (config == null) {
            config = HBaseConfiguration.create();
            config.set("hbase.zookeeper.quorum", "localhost:2181");
        }
    }

    private  Connection getConnection(){
        if (conn==null || conn.isClosed()){
            try{
                conn = ConnectionFactory.createConnection(config);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        return conn;
    }

    /**
     * 获取HBase连接
     * @return
     */
    public static Connection getHBaseConn(){
        return instance.getConnection();
    }

    /**
     * 获取HBase表
     * @param tableName
     * @return
     * @throws IOException
     */
    public static Table getTable(String tableName) throws IOException {
        return instance.getConnection().getTable(TableName.valueOf(tableName));
    }

    /**
     * 关闭连接
     * @throws Exception
     */
    public static void closeConn() throws Exception{
        if (conn!=null){
            conn.close();
        }
    }
}
