package com.veritas.hrt.runnables;

import com.veritas.hrt.utils.TableData;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class Runner extends Thread {

    private int id;
    private Connection conn;
    private int cooldownTime;
    private int fireCount;
    private CountDownLatch latch;

    public Runner(int id, Connection connection, int cooldownTime, int fireCount, CountDownLatch latch) {
        this.id = id;
        this.conn = connection;
        this.cooldownTime = cooldownTime;
        this.fireCount = fireCount;
        this.latch = latch;
    }

    private int getCounter(byte[] row, Table table) throws IOException {
        Get g = new Get(row);
        Result r = table.get(g);
        byte[] data = r.getValue(
                Bytes.toBytes(TableData.COUNTER_NAME),
                Bytes.toBytes(TableData.COUNTER_NAME)
        );
        return Integer.valueOf(Bytes.toString(data));
    }

    private void updateCounter(byte[] row, Table table, int counter) throws IOException {
        Put p = new Put(row);
        p.addImmutable(
                Bytes.toBytes(TableData.COUNTER_NAME),
                Bytes.toBytes(TableData.COUNTER_NAME),
                Bytes.toBytes(String.valueOf(counter + 1)));
        table.put(p);
    }

    @Override
    public void run() {

        try {
            TableName tableName = TableName.valueOf(TableData.TABLE_NAME);
            Table table = conn.getTable(tableName);

            for (int i=1; i<= fireCount; i++) {
                byte[] row = Bytes.toBytes(TableData.ROW_NAME);
                int counter = getCounter(row, table);
                Put p = new Put(row);
                String newData = String.format("runner #%d >> #%d", id, counter);
                p.addImmutable(
                        Bytes.toBytes(TableData.FAMILY_NAME),
                        Bytes.toBytes(String.valueOf(id)),
                        Bytes.toBytes(newData));
                table.put(p);
                System.out.printf("> Put '%s' to the row '%s'%n", newData, TableData.ROW_NAME);
                updateCounter(row, table, counter);
                Thread.sleep(cooldownTime);
            }

            latch.countDown();
        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }

    }
}
