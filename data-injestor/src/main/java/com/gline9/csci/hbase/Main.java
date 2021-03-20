package com.gline9.csci.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class Main
{
    public static void main(String[] args) throws IOException
    {

        Configuration configuration = HBaseConfiguration.create();
        try (Connection connection = ConnectionFactory.createConnection(configuration))
        {
            printStats(connection);
        }
    }

    public static void printStats(Connection connection) throws IOException {
        // question 1
        Table reviewTable = connection.getTable(TableName.valueOf("reviews"));

        Scan scan = new Scan();

        scan.addColumn(Bytes.toBytes("r"), Bytes.toBytes("rating"));

        ResultScanner reviewScan = reviewTable.getScanner(scan);

        for (Result result = reviewScan.next(); result != null; result = reviewScan.next()) {
            System.out.println(result);
            break;
        }

        reviewScan.close();
    }
}
