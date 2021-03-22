package com.gline9.csci.hbase;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

public class Main {
    public static void main(String[] args) throws IOException {

        Configuration configuration = HBaseConfiguration.create();
        try (Connection connection = ConnectionFactory.createConnection(configuration)) {
            //countNullValues(connection);
            //findCorrelation(connection);
            //findRelationship(connection);
            testNull(connection);
        }
    }

    public static void countNullValues(Connection connection) throws IOException {
        Table reviewTable = connection.getTable(TableName.valueOf("reviews"));
        Table metadataTable = connection.getTable(TableName.valueOf("metadata"));

        Scan reviewsScan = new Scan();
        Scan ratingScan = new Scan();
        Scan summaryScan = new Scan();
        Scan reviewerIDScan = new Scan();

        Scan titleScan = new Scan();
        Scan priceScan = new Scan();
        Scan brandScan = new Scan();


        //Review Dataset review column family and columns
        byte[] ratingFamily = Bytes.toBytes("r");
        byte[] ratingColumn = Bytes.toBytes("rating");
        byte[] reviewColumn = Bytes.toBytes("review");
        byte[] summaryColumn = Bytes.toBytes("summary");
        byte[] reviewerIDColumn = Bytes.toBytes("reviewerID");

        //Metadata dataset metadata column family and columns
        byte[] metadataFamily = Bytes.toBytes("m");
        byte[] titleColumn = Bytes.toBytes("title");
        byte[] priceColumn = Bytes.toBytes("price");
        byte[] brandColumn = Bytes.toBytes("brand");

        ratingScan.addColumn(ratingFamily, ratingColumn);
        reviewsScan.addColumn(ratingFamily, reviewColumn);
        reviewerIDScan.addColumn(ratingFamily, reviewerIDColumn);
        summaryScan.addColumn(ratingFamily, summaryColumn);

        titleScan.addColumn(metadataFamily, titleColumn);
        priceScan.addColumn(metadataFamily, priceColumn);
        brandScan.addColumn(metadataFamily, brandColumn);

        ResultScanner reviewScanner = reviewTable.getScanner(reviewsScan);
        ResultScanner ratingScanner = reviewTable.getScanner(ratingScan);
        ResultScanner reviewerIDScanner = reviewTable.getScanner(reviewerIDScan);
        ResultScanner summaryScanner = reviewTable.getScanner(summaryScan);

        ResultScanner titleScanner = metadataTable.getScanner(titleScan);
        ResultScanner priceScanner = metadataTable.getScanner(priceScan);
        ResultScanner brandScanner = metadataTable.getScanner(brandScan);

        int nullReviewCount = 0;
        int nullRatingCount = 0;
        int nullReviewerIDCount = 0;
        int nullSummaryCount = 0;
        int nullTitleCount = 0;
        int nullPriceCount = 0;
        int nullBrandCount = 0;


        for (Result result = reviewScanner.next(); result != null; result = reviewScanner.next()) {
            String review = Bytes.toString(result.getValue(ratingFamily, reviewColumn));

            if (review.equals("")) {
                nullReviewCount += 1;
            }
        }
        for (Result result = ratingScanner.next(); result != null; result = ratingScanner.next()) {
            short rating = Bytes.toShort(result.getValue(ratingFamily, ratingColumn));
            if (rating == 0) {
                nullRatingCount += 1;
            }
        }
        for (Result result = reviewerIDScanner.next(); result != null; result = reviewerIDScanner.next()) {
            String reviewerID = Bytes.toString(result.getValue(ratingFamily, reviewerIDColumn));
            if (reviewerID.equals("")) {
                nullReviewerIDCount += 1;
            }
        }
        for (Result result = summaryScanner.next(); result != null; result = summaryScanner.next()) {
            String summary = Bytes.toString(result.getValue(metadataFamily, summaryColumn));
            if (summary.equals("")) {
                nullSummaryCount += 1;
            }
        }
        for (Result result = titleScanner.next(); result != null; result = titleScanner.next()) {
            String title = Bytes.toString(result.getValue(metadataFamily, titleColumn));

            if (title.equals("")) {
                nullTitleCount += 1;
            }


        }
        for (Result result = priceScanner.next(); result != null; result = priceScanner.next()) {
            double price = Bytes.toDouble(result.getValue(metadataFamily, priceColumn));
            if (price == 0) {
                nullPriceCount += 1;
            }
        }
        for (Result result = brandScanner.next(); result != null; result = brandScanner.next()) {
            String brand = Bytes.toString(result.getValue(metadataFamily, brandColumn));
            if (brand.equals("")) {
                nullBrandCount += 1;
            }
        }

        System.out.println("     Printing stats");
        System.out.println("-------------------------");
        System.out.println("Null Review Count: " + nullReviewCount);
        System.out.println("Null Rating Count: " + nullRatingCount);
        System.out.println("Null Reviewer Count: " + nullReviewerIDCount);
        System.out.println("Null Summary Count: " + nullSummaryCount);
        System.out.println("Null Title Count: " + nullTitleCount);
        System.out.println("Null Price Count: " + nullPriceCount);
        System.out.println("Null Brand Count: " + nullBrandCount);
        System.out.println("-------------------------");

    }

    public static void findCorrelation(Connection connection) throws IOException {
        Table reviewTable = connection.getTable(TableName.valueOf("reviews"));
        //Table metadataTable = connection.getTable(TableName.valueOf("metadata"));
        Scan ratingScanner = new Scan();
        byte[] ratingFamily = Bytes.toBytes("r");
        byte[] ratingColumn = Bytes.toBytes("rating");
        ResultScanner ratingScan = reviewTable.getScanner(ratingScanner);
        for (Result result = ratingScan.next(); result != null; result = ratingScan.next()) {
            short rating = Bytes.toShort(result.getValue(ratingFamily, ratingColumn));

            System.out.println("Result: " + result);
            System.out.println("Rating - " + rating);
        }
    }

    public static void findRelationship(Connection connection) throws IOException {
        Table reviewTable = connection.getTable(TableName.valueOf("reviews"));
        Table metadataTable = connection.getTable(TableName.valueOf("metadata"));

        Scan reviewScan = new Scan();
        Scan metadataScan = new Scan();

        byte[] ratingFamily = Bytes.toBytes("r");
        byte[] ratingColumn = Bytes.toBytes("rating");
        byte[] metadataFamily = Bytes.toBytes("m");
        byte[] priceColumn = Bytes.toBytes("price");

        reviewScan.addColumn(ratingFamily, ratingColumn);
        metadataScan.addColumn(metadataFamily, priceColumn);

        ResultScanner reviewScanner = reviewTable.getScanner(reviewScan);
        ResultScanner metadataScanner = metadataTable.getScanner(metadataScan);


        int check = 0;
        for (Result result = metadataScanner.next(); result != null; result = metadataScanner.next()) {
            String metadataKey = Bytes.toString(result.getRow());
            double price = Bytes.toDouble(result.getValue(metadataFamily, priceColumn));
            System.out.println("MetadataKey - " + metadataKey + "\nPrice - " + price);
            if (++check == 3) {
                break;
            }
        }
        for (Result result = reviewScanner.next(); result != null; result = reviewScanner.next()) {
            String reviewKey = Bytes.toString(result.getRow());
            short rating = Bytes.toShort(result.getValue(ratingFamily, ratingColumn));
            String[] tmp = reviewKey.split("-", 2);
            reviewKey = tmp[0];
            System.out.println("ReviewKEY - " + reviewKey + "\nVALUE - " + rating);
            if (++check == 6) {
                break;
            }
        }

    }

    public static void testNull(Connection connection) throws IOException {
        Table reviewTable = connection.getTable(TableName.valueOf("reviews"));
        Table metadataTable = connection.getTable(TableName.valueOf("metadata"));

        Scan reviewScan = new Scan();
        Scan metadataScan = new Scan();

        //Review Dataset review column family and columns
        byte[] ratingFamily = Bytes.toBytes("r");
        byte[] ratingColumn = Bytes.toBytes("rating");
        byte[] reviewColumn = Bytes.toBytes("review");
        byte[] summaryColumn = Bytes.toBytes("summary");
        byte[] reviewerIDColumn = Bytes.toBytes("reviewerID");

        //Metadata dataset metadata column family and columns
        byte[] metadataFamily = Bytes.toBytes("m");
        byte[] categoryFamily = Bytes.toBytes("c");
        byte[] viewedFamily = Bytes.toBytes("v");
        byte[] boughtFamily = Bytes.toBytes("b");

        byte[] titleColumn = Bytes.toBytes("title");
        byte[] priceColumn = Bytes.toBytes("price");
        byte[] brandColumn = Bytes.toBytes("brand");

        int nullReviewCount = 0;
        int nullRatingCount = 0;
        int nullReviewerIDCount = 0;
        int nullSummaryCount = 0;
        int nullTitleCount = 0;
        int nullPriceCount = 0;
        int nullBrandCount = 0;

        ResultScanner reviewScanner = reviewTable.getScanner(reviewScan);
        ResultScanner metadataScanner = metadataTable.getScanner(metadataScan);
        for (Result result : reviewScanner) {
            if(Bytes.toShort(result.getValue(ratingFamily, ratingColumn)) == 0){
                //System.out.println("Null rating count incremented.");
                nullRatingCount += 1;
            }
            if(Bytes.toString(result.getValue(ratingFamily, reviewColumn)) == null){
                nullReviewCount += 1;
                //System.out.println("Null review count incremented.");

            }
            if(Bytes.toString(result.getValue(ratingFamily, reviewerIDColumn)) == null){
                nullReviewerIDCount += 1;
                //System.out.println("Null reviewerID count incremented.");

            }
            if(Bytes.toString(result.getValue(ratingFamily, summaryColumn)) == null){
                nullSummaryCount += 1;
                //System.out.println("Null summary count incremented.");

            }
        }
        for(Result result : metadataScanner){
            if(Bytes.toString(result.getValue(metadataFamily, titleColumn)) == null){
                nullTitleCount += 1;
                //System.out.println("Null title count incremented.");

            }
            if(Bytes.toDouble(result.getValue(metadataFamily, priceColumn)) == 0){
                nullPriceCount += 1;
                //System.out.println("Null price count incremented.");

            }
            if(Bytes.toString(result.getValue(metadataFamily, brandColumn)) == null){
                nullBrandCount += 1;
                //System.out.println("Null brand count incremented.");

            }
        }


        System.out.println("     Printing stats");
        System.out.println("-------------------------");
        System.out.println("Null Review Count: " + nullReviewCount);
        System.out.println("Null Rating Count: " + nullRatingCount);
        System.out.println("Null Reviewer Count: " + nullReviewerIDCount);
        System.out.println("Null Summary Count: " + nullSummaryCount);
        System.out.println("Null Title Count: " + nullTitleCount);
        System.out.println("Null Price Count: " + nullPriceCount);
        System.out.println("Null Brand Count: " + nullBrandCount);
        System.out.println("-------------------------");
    }

}