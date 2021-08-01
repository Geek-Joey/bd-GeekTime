package learn.bd.hadoop.hbase;

/**
 * @author Joey
 * @create 2021-08-01 22:46
 */
public class TestHBaseAPI {
    public static void main(String[] args) {
        String tableName="student";
        String[] colfmly = {"info","score"};
        HBaseUtils.createTable(tableName,colfmly);
        HBaseUtils.putRow(tableName,"wangxiangcheng",colfmly[0],"stduent_id","G20210712010190");
        HBaseUtils.putRow(tableName,"wangxiangcheng",colfmly[0],"class","1");
        HBaseUtils.putRow(tableName,"wangxiangcheng",colfmly[1],"understanding","60");
        HBaseUtils.putRow(tableName,"wangxiangcheng",colfmly[1],"programming","60");

    }
}
