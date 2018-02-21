package com.veritas.hrt.runnables;

import com.veritas.hrt.utils.HBaseConnectionProvider;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class Hrtest {

    public static void main(String[] args) throws IOException {

        Connection conn = HBaseConnectionProvider.getInstance().getConnection();
        Admin admin = conn.getAdmin();

        String TABLE_NAME = "test_table";
        String FIRST_FAMILY = "family_1";
        String SECOND_FAMILY = "family_2";
        String FIRST_QUALIFIER = "qualifier_a";
        String SECOND_QUALIFIER = "qualifier_b";

        TableName testTableName = TableName.valueOf(TABLE_NAME);
        HTableDescriptor descriptor = new HTableDescriptor(testTableName);
        descriptor.addFamily(new HColumnDescriptor(FIRST_FAMILY));
        descriptor.addFamily(new HColumnDescriptor(SECOND_FAMILY));

        if (admin.tableExists(testTableName)) {
            admin.disableTable(testTableName);
            admin.deleteTable(testTableName);
        }

        admin.createTable(descriptor);
        System.out.println("Created table: " + testTableName);
        System.out.println(admin.getTableDescriptor(testTableName));

        Table testTable = conn.getTable(testTableName);

        byte[] row1 = Bytes.toBytes("test_row_name");
        Put p = new Put(row1);
        p.addImmutable(FIRST_FAMILY.getBytes(), FIRST_QUALIFIER.getBytes(), String.format("%s:%s", FIRST_FAMILY, FIRST_QUALIFIER).getBytes());
        testTable.put(p);

        Get g = new Get(row1);
        Result r = testTable.get(g);
        byte[] retrievedData = r.getValue(FIRST_FAMILY.getBytes(), FIRST_QUALIFIER.getBytes());
        System.out.println(Bytes.toString(retrievedData));

        testTable.close();
        conn.close();

    }

}
