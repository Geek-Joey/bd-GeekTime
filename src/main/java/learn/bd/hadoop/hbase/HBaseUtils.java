package learn.bd.hadoop.hbase;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Arrays;

/**
 * HBase工具类
 * @author Joey
 * @create 2021-08-01 22:35
 */
public class HBaseUtils {

    /**
     * 创建表
     * @param tableName
     * @param cfs
     * @return
     */
    public static boolean createTable(String tableName, String[] cfs) {
        try (HBaseAdmin admin = (HBaseAdmin) HBaseConn.getHBaseConn().getAdmin()) {
            if (admin.tableExists(TableName.valueOf(tableName))) {
                return false;
            }
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            Arrays.stream(cfs).forEach(cf -> {
                HColumnDescriptor columnDescriptor = new HColumnDescriptor(cf);
                columnDescriptor.setMaxVersions(1);
                tableDescriptor.addFamily(columnDescriptor);
            });
            admin.createTable(tableDescriptor);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }

    /**
     * 删除表
     * @param tableName
     * @return
     */
    public static boolean deleteTable(String tableName){
        try(HBaseAdmin admin = (HBaseAdmin)HBaseConn.getHBaseConn().getAdmin()){
            if (!admin.tableExists(TableName.valueOf(tableName))){
                return false;
            }
            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));
        }catch (Exception e){
            e.printStackTrace();
        }
        return true;
    }

    /**
     * 插入数据
     * @param tableName
     * @param rowkey
     * @param cfName
     * @param qualifer
     * @param data
     * @return
     */
    public static boolean putRow(String tableName,String rowkey,String cfName,String qualifer,String data){
        try(Table table = HBaseConn.getTable(tableName)){
            Put put = new Put(Bytes.toBytes(rowkey));
            put.addColumn(Bytes.toBytes(cfName),Bytes.toBytes(qualifer),Bytes.toBytes(data));
            table.put(put);
        }catch (Exception e){
            e.printStackTrace();
        }
        return true;
    }

    /**
     * 查询数据
     * @param tableName
     * @param rowkey
     * @return
     */
    public static Result getRow(String tableName,String rowkey){
        try( Table table = HBaseConn.getTable(tableName)){
            Get get = new Get(Bytes.toBytes(rowkey));
            return table.get(get);
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }

    /**
     *扫描全表数据
     * @param tableName
     * @return
     */
    public static ResultScanner getScanner(String tableName){
        try( Table table = HBaseConn.getTable(tableName)){
            Scan scan = new Scan();
            scan.setCaching(1000);
            ResultScanner results = table.getScanner(scan);
            results.forEach(result -> {
                System.out.println("rowkey == "+Bytes.toString(result.getRow()));
                System.out.println("basic:name == "+Bytes.toString(result.getValue(Bytes.toBytes("basic"), Bytes.toBytes("name"))));
                System.out.println("basic:age == "+Bytes.toString(result.getValue(Bytes.toBytes("basic"), Bytes.toBytes("age"))));
                System.out.println("basic:sex == "+Bytes.toString(result.getValue(Bytes.toBytes("basic"), Bytes.toBytes("sex"))));
                System.out.println("basic:salary == "+Bytes.toString(result.getValue(Bytes.toBytes("extend"), Bytes.toBytes("salary"))));
                System.out.println("basic:job == "+Bytes.toString(result.getValue(Bytes.toBytes("extend"), Bytes.toBytes("job"))));
            });
            return results;
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 删除行
     * @param tableName
     * @param rowkey
     * @return
     */
    public static boolean deleteRow(String tableName,String rowkey){
        try( Table table = HBaseConn.getTable(tableName)){
            Delete delete = new Delete(Bytes.toBytes(rowkey));
            table.delete(delete);
        }catch (Exception e){
            e.printStackTrace();
        }
        return true;
    }

    /**
     * 删除列
     * @param tableName
     * @param cfName
     * @return
     */
    public static boolean deleteQualifier(String tableName,String rowkey,String cfName,String qualiferName){
        try(Table table = HBaseConn.getTable(tableName)){
            Delete delete = new Delete(Bytes.toBytes(rowkey));
            delete.addColumn(Bytes.toBytes(cfName),Bytes.toBytes(qualiferName));
            table.delete(delete);
        }catch (Exception e){
            e.printStackTrace();
        }
        return true;
    }

}
