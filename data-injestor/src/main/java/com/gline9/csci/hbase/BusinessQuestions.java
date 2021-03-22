package com.gline9.csci.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;

public class BusinessQuestions {
    public static void main(String[] args) throws IOException {

        Configuration configuration = HBaseConfiguration.create();
        try (Connection connection = ConnectionFactory.createConnection(configuration)) {
            printReviewStats(connection);

        }
    }
    public static void countNullValues(Connection connection) throws IOException{

    }
    public static double[] getReviewAverage(long[] reviews) {
        long total = 0;

        for (int i = 0; i < 6; i++) {
            total += reviews[i];
        }

        double[] averages = new double[2];

        for (int i = 1; i < 6; i++) {
            averages[0] += (double) i * reviews[i] / total;
            averages[1] += (double) i * reviews[i] / (total - reviews[0]);
        }

        return averages;
    }
    public static void printReviewStats(Connection connection) throws IOException {
        // question 1 (easy) - reviews
        System.out.println("Printing review rating stats.");

        Table reviewTable = connection.getTable(TableName.valueOf("reviews"));

        Scan scan = new Scan();

        byte[] ratingFamily = Bytes.toBytes("r");

        byte[] ratingColumn = Bytes.toBytes("rating");

        scan.addColumn(ratingFamily, ratingColumn);

        ResultScanner reviewScan = reviewTable.getScanner(scan);

        // assuming that reviews can only be 0, 1, 2, 3, 4, 5
        long[] reviews = new long[6];

        for (Result result = reviewScan.next(); result != null; result = reviewScan.next()) {
            short tmp = Bytes.toShort(result.getValue(ratingFamily, ratingColumn));
            reviews[tmp] += 1;
        }

        double[] averages = getReviewAverage(reviews);

        System.out.println("Review information: avg: " + averages[0] + ", avg (no zeroes): " + averages[1]);

        for (int i = 0; i < 6; i++) {
            System.out.println("Review count for " + i + ": " + reviews[i]);
        }

        reviewScan.close();
    }
}