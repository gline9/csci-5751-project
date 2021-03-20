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
            printReviewStats(connection);
        }
    }

    public static double getReviewAverage(long[] reviews) {
        long total = 0;

        for (int i = 0; i < 6; i++) {
            total += reviews[i];
        }

        double average = 0;

        for (int i = 1; i < 6; i++) {
            average += (double) i * reviews[i] / total;
        }

        return average;
    }

    public static void printReviewStats(Connection connection) throws IOException {
        // question 1
        Table reviewTable = connection.getTable(TableName.valueOf("reviews"));

        Scan scan = new Scan();

        byte[] ratingFamily = Bytes.toBytes("r");

        byte[] ratingColumn = Bytes.toBytes("rating");

        scan.addColumn(ratingFamily, ratingColumn);

        ResultScanner reviewScan = reviewTable.getScanner(scan);

        short reviewMin = 6;
        short reviewMax = -1;

        // assuming that reviews can only be 0, 1, 2, 3, 4, 5
        long[] reviews = new long[6];

        for (Result result = reviewScan.next(); result != null; result = reviewScan.next()) {
            short tmp = Bytes.toShort(result.getValue(ratingFamily, ratingColumn));
            if (tmp > reviewMax) {
                reviewMax = tmp;
            }
            if (tmp < reviewMin) {
                reviewMin = tmp;
            }
            reviews[tmp] += 1;
        }

        System.out.println("Review information: min: "
                        + reviewMin + " max: " + reviewMax + " avg: " + getReviewAverage(reviews));

        reviewScan.close();
    }
}
