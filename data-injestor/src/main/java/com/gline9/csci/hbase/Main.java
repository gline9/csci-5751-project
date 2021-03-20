package com.gline9.csci.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Main {
    public static void main(String[] args) throws IOException {

        Configuration configuration = HBaseConfiguration.create();
        try (Connection connection = ConnectionFactory.createConnection(configuration)) {
            //printReviewStats(connection);
            //printPriceStats(connection);
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
        // question 1 (easy) - reviews
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

        for (int i = 0; i < 6; i++) {
            System.out.println("Review count for " + i + ": " + reviews[i]);
        }

        reviewScan.close();
    }

    public static double getPriceAverage(HashMap<Double, Long> priceMap) {
        long total = 0;
        double average = 0.0;

        for (Long count : priceMap.values())  {
            total += count;
        }

        for (HashMap.Entry<Double, Long> entry : priceMap.entrySet()) {
            average += entry.getKey() * entry.getValue() / total;
        }

        return average;
    }

    public static void printPriceStats(Connection connection) throws IOException {
        // question 1 (easy) - price
        Table metadataTable = connection.getTable(TableName.valueOf("metadata"));

        Scan scan = new Scan();

        byte[] metadataFamily = Bytes.toBytes("m");

        byte[] metadataColumn = Bytes.toBytes("price");

        scan.addColumn(metadataFamily, metadataColumn);

        ResultScanner priceScan = metadataTable.getScanner(scan);

        // assuming minPrice is less than 100.00
        double priceMin = 100.0;
        double priceMax = 0.0;

        HashMap<Double, Long> priceMap = new HashMap<>();

        for (Result result = priceScan.next(); result != null; result = priceScan.next()) {
            double tmp = Bytes.toDouble(result.getValue(metadataFamily, metadataColumn));
            if (tmp > priceMax) {
                priceMax = tmp;
            }
            if (tmp < priceMin) {
                priceMin = tmp;
            }

            if (priceMap.containsKey(tmp)) {
                priceMap.put(tmp, priceMap.get(tmp) + 1);
            } else {
                priceMap.put(tmp, 1L);
            }
        }

        System.out.println("Price information: min: "
                + priceMin + " max: " + priceMax + " avg: " + getPriceAverage(priceMap));

        priceScan.close();

    }

    public static void printTopBrandPerCategory(Connection connection) throws IOException {
        // question 3 (medium)
        Table brandReviewsTable = connection.getTable(TableName.valueOf("brandReviews"));

        Scan scan = new Scan();

        byte[] brandReviewsFamily = Bytes.toBytes("o");

        byte[] brandReviewsColumn = Bytes.toBytes("overall");

        scan.addColumn(brandReviewsFamily, brandReviewsColumn);

        ResultScanner brandReviewsScan = brandReviewsTable.getScanner(scan);

        // Category hashmap that contains brand hashmap, which contains a rating hashmap
        HashMap<String, HashMap<String, HashMap<Short, Long>>> catBrandReviewsMap = new HashMap<>();

        // first, get all the review ratings, brands, and categories and add them to the map
        for (Result result = brandReviewsScan.next(); result != null; result = brandReviewsScan.next()) {
            short tmp = Bytes.toShort(result.getValue(brandReviewsFamily, brandReviewsColumn));

            String rowKey = Bytes.toString(result.getRow());
            // should be category-brand
            String[] splitRowKey = rowKey.split("-");
            if (catBrandReviewsMap.containsKey(splitRowKey[0])) {
                HashMap<String, HashMap<Short, Long>> brandReviewsMap = catBrandReviewsMap.get(splitRowKey[0]);
                if (brandReviewsMap.containsKey(splitRowKey[1])) {
                    HashMap<Short, Long> reviewsMap = brandReviewsMap.get(splitRowKey[1]);
                    if (reviewsMap.containsKey(tmp)) {
                        // we have brand, category, and rating
                        reviewsMap.put(tmp, reviewsMap.get(tmp) + 1);
                    } else {
                        // we have brand, category, but not rating
                        reviewsMap.put(tmp, 1L);
                    }
                } else {
                    // we have the category, but not the brand or rating
                    HashMap<Short, Long> reviewsMap = new HashMap<>();
                    reviewsMap.put(tmp, 1L);
                    brandReviewsMap.put(splitRowKey[1], reviewsMap);
                }
            } else {
                // we don't have the category or the brand or rating
                HashMap<Short, Long> reviewsMap = new HashMap<>();
                reviewsMap.put(tmp, 1L);
                HashMap<String, HashMap<Short, Long>> brandReviewsMap = new HashMap<>();
                brandReviewsMap.put(splitRowKey[1], reviewsMap);
                catBrandReviewsMap.put(splitRowKey[0], brandReviewsMap);
            }
        }

        // loop through the categories, finding the top 3 brands for each

        brandReviewsScan.close();
    }
}
