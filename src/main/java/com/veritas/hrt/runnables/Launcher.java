package com.veritas.hrt.runnables;

import com.veritas.hrt.utils.HBaseConnectionProvider;
import com.veritas.hrt.utils.TableData;
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
import java.util.concurrent.CountDownLatch;

public class Launcher {

    public static void main(String[] args) throws IOException, InterruptedException {

        int cooldownTime = 1000;
        int fires = 2;
        int runnersCount = 3;
        CountDownLatch latch = new CountDownLatch(runnersCount);

        Connection connection = HBaseConnectionProvider.getInstance().getConnection();
        Admin admin = connection.getAdmin();

        TableName tableName = TableName.valueOf(TableData.TABLE_NAME);
        Table table = connection.getTable(tableName);
        byte[] row = Bytes.toBytes(TableData.ROW_NAME);

        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.printf("> Existing table '%s' was deleted%n", TableData.TABLE_NAME);
        }

        HTableDescriptor descriptor = new HTableDescriptor(tableName);
        descriptor.addFamily(new HColumnDescriptor(TableData.FAMILY_NAME));
        descriptor.addFamily(new HColumnDescriptor(TableData.COUNTER_NAME));
        admin.createTable(descriptor);
        System.out.printf("> Created table '%s'%n%s%n", TableData.TABLE_NAME, descriptor);


        Put p = new Put(row);
        p.addImmutable(
                Bytes.toBytes(TableData.COUNTER_NAME),
                Bytes.toBytes(TableData.COUNTER_NAME),
                Bytes.toBytes(String.valueOf(1)));
        table.put(p);

        for (int i=1; i<=runnersCount; i++) {
            new Runner(i, connection, cooldownTime, fires, latch).start();
            Thread.sleep(cooldownTime / runnersCount);
        }

        latch.await();

        Get g = new Get(row);
        Result r = table.get(g);
        System.out.printf("> Retrieved the final row: %s%n", r);

        String[] values = new String[runnersCount];
        for (int i=0; i<runnersCount; i++) {
            byte[] data = r.getValue(
                    Bytes.toBytes(TableData.FAMILY_NAME),
                    Bytes.toBytes(String.valueOf(i+1))
            );
            System.out.printf("%s:%s -> '%s'%n", TableData.FAMILY_NAME, i+1, Bytes.toString(data));
        }

        table.close();
        connection.close();

    }

}
