package com.gline9.csci.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;

public class Main {
    public static void main(String[] args) throws IOException {

        Configuration configuration = HBaseConfiguration.create();
        try (Connection connection = ConnectionFactory.createConnection(configuration)) {
            // much specifiy a percent of data to sample
            switch (args[0]) {
                case "1r":
                    printReviewStats(connection);
                    break;
                case "1p":
                    printPriceStats(connection);
                    break;
                case "7":
                    writeTopBrandPerCategory(connection);
                    break;
                case "8":
                    writeReviewSample(connection, Double.parseDouble(args[1]));
                    break;
            }
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
        System.out.println("Printing review rating stats.");

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
        System.out.println("Printing price stats.");

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

    public static double getReviewMapAverage(HashMap<Short, Long> reviewMap) {
        long total = 0;
        double average = 0.0;

        for (Long count : reviewMap.values())  {
            total += count;
        }

        for (HashMap.Entry<Short, Long> entry : reviewMap.entrySet()) {
            average += ((double) entry.getKey()) * entry.getValue() / total;
        }

        return average;
    }

    public static void writeTopThree(
            String category,
            HashMap<String, HashMap<Short, Long>> brandMap,
            FileWriter writer) throws IOException {
        // get the top three, per the input category

        // prices organized from high to low
        double[] prices = new double[3];
        String[] brands = new String[3];

        for (HashMap.Entry<String, HashMap<Short, Long>> entry : brandMap.entrySet()) {
            double average = getReviewMapAverage(entry.getValue());
            // lazy hardcoded method, manually swap down the values
            if (average > prices[0]) {
                // new largest value
                prices[2] = prices[1];
                brands[2] = brands[1];
                prices[1] = prices[0];
                brands[1] = brands[0];
                prices[0] = average;
                brands[0] = entry.getKey();
            } else if (average > prices[1]) {
                // new second largest
                prices[2] = prices[1];
                brands[2] = brands[1];
                prices[1] = average;
                brands[1] = entry.getKey();
            } else if (average > prices[2]) {
                // new third largest
                prices[2] = average;
                brands[2] = entry.getKey();
            }
        }

        writer.write("Category: " + category + ", Brand/price: "
                + brands[0] + "/" + prices[0] + ", "
                + brands[1] + "/" + prices[1] + ", "
                + brands[2] + "/" + prices[2] + "\n");
    }

    public static void writeTopBrandPerCategory(Connection connection) throws IOException {
        // question 3 (medium)
        System.out.print("Attempting to scan database and retrieve top brands per category.");
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

        System.out.println("Finished scan of database, starting aggregation.");
        // loop through the categories, finding the top 3 brands for each
        // output to file as each calculation is made
        FileWriter writer = new FileWriter("/json-data/top-brands-per-category.txt");
        for (HashMap.Entry<String, HashMap<String, HashMap<Short, Long>>> entry : catBrandReviewsMap.entrySet()) {
            writeTopThree(entry.getKey(), entry.getValue(), writer);
        }
        writer.close();

        System.out.println("Finished write of top brands per category to /json-data/top-brands-per-category.txt.");

        brandReviewsScan.close();
    }

    public static void writeReviewSample(Connection connection, double percent) throws IOException {
        // question 3 (medium), supply a percent to sample a certain percent of the data (approximate)
        System.out.println("Sampling train and test data for fastText model.");
        Table reviewsTable = connection.getTable(TableName.valueOf("reviews"));

        Scan scan = new Scan();

        byte[] reviewFamily = Bytes.toBytes("r");

        byte[] reviewColumn = Bytes.toBytes("review");

        byte[] ratingColumn = Bytes.toBytes("rating");

        scan.addColumn(reviewFamily, reviewColumn);
        scan.addColumn(reviewFamily, ratingColumn);

        ResultScanner reviewScan = reviewsTable.getScanner(scan);
        FileWriter trainWriter = new FileWriter("/json-data/reviews.train");
        FileWriter testWriter = new FileWriter("/json-data/reviews.test");

        for (Result result = reviewScan.next(); result != null; result = reviewScan.next()) {
            short rating = Bytes.toShort(result.getValue(reviewFamily, ratingColumn));
            String review = Bytes.toString(result.getValue(reviewFamily, reviewColumn));
            double random = Math.random();
            if (random < percent) {
                if (random < percent * 0.8) {
                    // training data
                    trainWriter.write("__lab__" + rating + " " + review + "\n");
                } else {
                    // testing data
                    testWriter.write("__lab__" + rating + " " + review + "\n");
                }
            }
        }
        trainWriter.close();
        testWriter.close();

        System.out.println("Finished writing fasttext input data.");

        reviewScan.close();

    }
}
