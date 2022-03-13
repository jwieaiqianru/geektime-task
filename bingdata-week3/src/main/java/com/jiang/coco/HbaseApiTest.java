package com.jiang.coco;

import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Hello world!
 */


public class HbaseApiTest {


    private static final String FAMILY_NAME = "name";
    private static final String FAMILY_INFO = "info";
    private static final String FAMILY_SCORE = "score";

    private static final int ROW_KEY = 1;

    public static void main(String[] args) throws IOException {
        Configuration configuration = HBaseConfiguration.create();

        Connection conn = ConnectionFactory.createConnection(configuration);
        Admin admin = conn.getAdmin();
        TableName tableName = TableName.valueOf("jiangwei:student");
        ColumnFamilyDescriptor name = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(FAMILY_NAME)).build();
        ColumnFamilyDescriptor info = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(FAMILY_INFO)).build();
        ColumnFamilyDescriptor score = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(FAMILY_SCORE)).build();

        TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName).setColumnFamily(name)
            .setColumnFamily(info).setColumnFamily(score).build();
        // 创建表
        if (admin.tableExists(tableName)) {
            System.out.println("table " + Arrays.toString(tableName.getName()) + " already exist");
        } else {
            admin.createTable(tableDescriptor);
        }
        // 插入数据
        Put put = new Put(Bytes.toBytes(ROW_KEY));
        put.addColumn(Bytes.toBytes(FAMILY_NAME), Bytes.toBytes(FAMILY_NAME), Bytes.toBytes("jiangwei"));
        put.addColumn(Bytes.toBytes(FAMILY_INFO), Bytes.toBytes("student_id"), Bytes.toBytes("G20210579030072"));
        put.addColumn(Bytes.toBytes(FAMILY_INFO), Bytes.toBytes("class"), Bytes.toBytes("1"));
        put.addColumn(Bytes.toBytes(FAMILY_SCORE), Bytes.toBytes("understanding"), Bytes.toBytes("100"));
        put.addColumn(Bytes.toBytes(FAMILY_SCORE), Bytes.toBytes("programming"), Bytes.toBytes("100"));

        conn.getTable(tableName).put(put);
        //获取数据
        Get get = new Get(Bytes.toBytes(ROW_KEY));
        if (!get.isCheckExistenceOnly()) {
            Result result = conn.getTable(tableName).get(get);
            for (Cell cell : result.rawCells()) {
                String colName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                System.out.println("Data get success, colName: " + colName + ", value: " + value);
            }
        }
        // 删除数据
        Delete delete = new Delete(Bytes.toBytes(ROW_KEY));      // 指定rowKey
        conn.getTable(tableName).delete(delete);
        System.out.println("Delete Success");

        // 删除表
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("Table Delete Successful");
        } else {
            System.out.println("Table does not exist!");
        }
    }
}
