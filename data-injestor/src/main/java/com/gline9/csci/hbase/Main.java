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
            findRelationship(connection);
        }
    }

    public static void countNullValues(Connection connection) throws IOException {
        Table reviewTable = connection.getTable(TableName.valueOf("reviews"));
        Table metadataTable = connection.getTable(TableName.valueOf("metadata"));
        Scan reviewsScan = new Scan();
        Scan metadataScan = new Scan();

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

        reviewsScan.addColumn(ratingFamily, ratingColumn);
        reviewsScan.addColumn(ratingFamily, reviewColumn);
        reviewsScan.addColumn(ratingFamily, reviewerIDColumn);
        reviewsScan.addColumn(ratingFamily, summaryColumn);
        metadataScan.addColumn(metadataFamily, titleColumn);
        metadataScan.addColumn(metadataFamily, priceColumn);
        metadataScan.addColumn(metadataFamily, brandColumn);

        ResultScanner reviewScanner = reviewTable.getScanner(reviewsScan);
        ResultScanner metadataScanner = metadataTable.getScanner(metadataScan);
        int nullReviewCount = 0;
        int nullRatingCount = 0;
        int nullReviewerIDCount = 0;
        int nullSummaryCount = 0;
        int nullTitleCount = 0;
        int nullPriceCount = 0;
        int nullBrandCount = 0;


        for (Result result = reviewScanner.next(); result != null; result = reviewScanner.next()) {
            String review = Bytes.toString(result.getValue(ratingFamily, reviewColumn));
            short rating = Bytes.toShort(result.getValue(ratingFamily, ratingColumn));
            String reviewerID = Bytes.toString(result.getValue(ratingFamily, reviewerIDColumn));
            String summary = Bytes.toString(result.getValue(metadataFamily, summaryColumn));

            if (review.equals("")) {
                nullReviewCount += 1;
            }
            if (reviewerID.equals("")) {
                nullReviewerIDCount += 1;
            }
            if (rating == 0) {
                nullRatingCount += 1;
            }
            if (summary.equals("")) {
                nullSummaryCount += 1;
            }
        }

        for (Result result = metadataScanner.next(); result != null; result = metadataScanner.next()) {
            String title = Bytes.toString(result.getValue(metadataFamily, titleColumn));
            double price = Bytes.toDouble(result.getValue(metadataFamily, priceColumn));
            String brand = Bytes.toString(result.getValue(metadataFamily, brandColumn));

            if (title.equals("")) {
                nullTitleCount += 1;
            }
            if (price == 0) {
                nullPriceCount += 1;
            }
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

    public static void findRelationship(Connection connection) throws IOException{
        Table reviewTable = connection.getTable(TableName.valueOf("reviews"));
        Table metadataTable = connection.getTable(TableName.valueOf("metadata"));

        Scan reviewScan = new Scan();
        Scan metadataScan = new Scan();

        byte[] ratingFamily = Bytes.toBytes("r");
        byte[] ratingColumn = Bytes.toBytes("rating");
        byte[] metadataFamily = Bytes.toBytes("m");
        byte[] priceColumn = Bytes.toBytes("price");

        reviewScan.addColumn(ratingFamily,ratingColumn);
        metadataScan.addColumn(metadataFamily,priceColumn);

        ResultScanner reviewScanner = reviewTable.getScanner(reviewScan);
        ResultScanner metadataScanner = metadataTable.getScanner(metadataScan);


        int check = 0;
        for(Result result = metadataScanner.next(); result != null; result = metadataScanner.next()){
            String metadataKey = Bytes.toString(result.getRow());
            double price = Bytes.toDouble(result.getValue(metadataFamily,priceColumn));
            System.out.println("MetadataKey - " + metadataKey + "Price - " + price);
            if(++check == 3){
                break;
            }
        }
        for(Result result = reviewScanner.next(); result != null; result = reviewScanner.next()){
            String reviewKey = Bytes.toString(result.getRow());
            short rating = Bytes.toShort(result.getValue(ratingFamily,ratingColumn));
            String[] tmp = reviewKey.split("-",2);
            reviewKey = tmp[0];
            System.out.println("ReviewKEY - " + reviewKey + "VALUE - " + rating);
            if(++check == 6){
                break;
            }
        }

    }
}