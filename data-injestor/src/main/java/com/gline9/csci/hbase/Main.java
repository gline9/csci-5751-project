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

    public static double[] getPriceAverage(HashMap<Double, Long> priceMap) {
        long total = 0;
        long totalNoZero = 0;
        double[] averages = new double[2];

        System.out.println("Printing high prices over 100000");
        for (HashMap.Entry<Double, Long> entry : priceMap.entrySet())  {
            total += entry.getValue();
            if (entry.getKey() != 0) {
                totalNoZero += entry.getValue();
            }
            if (entry.getKey() > 100000) {
                System.out.println(entry.getKey());
            }
        }

        for (HashMap.Entry<Double, Long> entry : priceMap.entrySet()) {
            averages[1] += entry.getKey() * entry.getValue() / totalNoZero;
            averages[0] += entry.getKey() * entry.getValue() / total;
        }

        return averages;
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

        // also keep track of nonzero min
        double priceMinNoZero = 100.0;

        HashMap<Double, Long> priceMap = new HashMap<>();

        for (Result result = priceScan.next(); result != null; result = priceScan.next()) {
            double tmp = Bytes.toDouble(result.getValue(metadataFamily, metadataColumn));

            if (tmp > priceMax) {
                priceMax = tmp;
            }
            if (tmp < priceMin) {
                priceMin = tmp;
            }
            if (tmp < priceMinNoZero && tmp != 0.0) {
                priceMinNoZero = tmp;
            }

            if (priceMap.containsKey(tmp)) {
                priceMap.put(tmp, priceMap.get(tmp) + 1);
            } else {
                priceMap.put(tmp, 1L);
            }
        }

        double[] averages = getPriceAverage(priceMap);

        System.out.println("Price information: min: "
                + priceMin + ", min (no zeroes): " + priceMinNoZero + ", max: " + priceMax +
                ", avg: " + averages[0] + ", avg (no zeroes): " + averages[1]);

        priceScan.close();
    }

    public static double[] getReviewMapAverage(HashMap<Short, Long> reviewMap) {
        long total = 0;
        long totalNoZero = 0;
        // first average is with zeros, second is without
        double[] averages = new double[2];

        for (HashMap.Entry<Short, Long> entry : reviewMap.entrySet())  {
            total += entry.getValue();
            if (entry.getValue() != 0) {
                totalNoZero += entry.getValue();
            }
        }

        for (HashMap.Entry<Short, Long> entry : reviewMap.entrySet()) {
            averages[0] += ((double) entry.getKey()) * entry.getValue() / total;
            // since the zero values contribute nothing, doesn't matter
            averages[1] += ((double) entry.getKey()) * entry.getValue() / totalNoZero;
        }

        return averages;
    }

    public static void writeTopThree(
            String category,
            HashMap<String, HashMap<Short, Long>> brandMap,
            FileWriter writer) throws IOException {
        // get the top three, per the input category

        // prices organized from high to low
        double[] prices = new double[3];
        String[] brands = new String[3];

        double[] pricesNoZero = new double[3];
        String[] brandsNoZero = new String[3];

        for (HashMap.Entry<String, HashMap<Short, Long>> entry : brandMap.entrySet()) {
            double[] averages = getReviewMapAverage(entry.getValue());
            // lazy hardcoded method, manually swap down the values
            // start with array with zeroes
            if (averages[0] > prices[0]) {
                // new largest value
                prices[2] = prices[1];
                brands[2] = brands[1];
                prices[1] = prices[0];
                brands[1] = brands[0];
                prices[0] = averages[0];
                brands[0] = entry.getKey();
            } else if (averages[0] > prices[1]) {
                // new second largest
                prices[2] = prices[1];
                brands[2] = brands[1];
                prices[1] = averages[0];
                brands[1] = entry.getKey();
            } else if (averages[0] > prices[2]) {
                // new third largest
                prices[2] = averages[0];
                brands[2] = entry.getKey();
            }

            // start with array without zeroes
            if (averages[1] > pricesNoZero[0]) {
                // new largest value
                pricesNoZero[2] = pricesNoZero[1];
                brandsNoZero[2] = brandsNoZero[1];
                pricesNoZero[1] = pricesNoZero[0];
                brandsNoZero[1] = brandsNoZero[0];
                pricesNoZero[0] = averages[1];
                brandsNoZero[0] = entry.getKey();
            } else if (averages[1] > pricesNoZero[1]) {
                // new second largest
                pricesNoZero[2] = pricesNoZero[1];
                brandsNoZero[2] = brandsNoZero[1];
                pricesNoZero[1] = averages[1];
                brandsNoZero[1] = entry.getKey();
            } else if (averages[1] > pricesNoZero[2]) {
                // new third largest
                pricesNoZero[2] = averages[1];
                brandsNoZero[2] = entry.getKey();
            }
        }

        writer.write("Category (with zeroes): " + category + ", Brand/price: "
                + brands[0] + "/" + prices[0] + ", "
                + brands[1] + "/" + prices[1] + ", "
                + brands[2] + "/" + prices[2] + "\n");

        writer.write("Category (without zeroes): " + category + ", Brand/price: "
                + brandsNoZero[0] + "/" + pricesNoZero[0] + ", "
                + brandsNoZero[1] + "/" + pricesNoZero[1] + ", "
                + brandsNoZero[2] + "/" + pricesNoZero[2] + "\n");
    }

    public static void writeTopBrandPerCategory(Connection connection) throws IOException {
        // question 3 (medium)
        System.out.print("Attempting to scan database and retrieve top brands per category.");
        Table brandReviewsTable = connection.getTable(TableName.valueOf("categoryReviews"));

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
            if (random < percent && rating != 0) {
                if (random < percent * 0.8) {
                    // training data
                    trainWriter.write("__label__" + rating + " " + review + "\n");
                } else {
                    // testing data
                    testWriter.write("__label__" + rating + " " + review + "\n");
                }
            }
        }
        trainWriter.close();
        testWriter.close();

        System.out.println("Finished writing fasttext input data.");

        reviewScan.close();

    }
}
