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
        }
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
        Table metadataTable = connection.getTable(TableName.valueOf("metadata"));

        Scan metadataScan = new Scan();

        byte[] metadataFamily = Bytes.toBytes("m");
        byte[] overallFamily = Bytes.toBytes("o");

        byte[] priceColumn = Bytes.toBytes("price");
        byte[] reviewerIDColumn = Bytes.toBytes("reviewerID");

        //metadataScan.addColumn(metadataFamily, priceColumn);
        //metadataScan.addColumn(overallFamily,reviewerIDColumn);

        ResultScanner metadataScanner = metadataTable.getScanner(metadataScan);

        for (Result result : metadataScanner) {
            String metadataKey = Bytes.toString(result.getRow());
            String[] tmp = metadataKey.split("-", 2);
            metadataKey = tmp[0];
            if (Bytes.toDouble(result.getValue(metadataFamily, priceColumn)) != 0 && Bytes.toShort(result.getValue(overallFamily,reviewerIDColumn)) != 0){
                double price = Bytes.toDouble(result.getValue(metadataFamily, priceColumn));
                int overall = Bytes.toShort(result.getValue(overallFamily,reviewerIDColumn));
            }

        }

    }

    public static void countNullValues(Connection connection) throws IOException {
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
            if(result.getValue(ratingFamily, ratingColumn) == null){
                //System.out.println("Rating - " + Bytes.toShort(result.getValue(ratingFamily, ratingColumn)));
                //System.out.println("Null rating count incremented.");
                nullRatingCount += 1;
            }
            if(result.getValue(ratingFamily, reviewColumn) == null){
                nullReviewCount += 1;
                //System.out.println("Review - " + Bytes.toString(result.getValue(ratingFamily, reviewColumn)));
                //System.out.println("Null review count incremented.");

            }
            if(result.getValue(ratingFamily, reviewerIDColumn) == null){
                nullReviewerIDCount += 1;
                //System.out.println("ReviewerID - " + Bytes.toString(result.getValue(ratingFamily, reviewerIDColumn)));
                //System.out.println("Null reviewerID count incremented.");

            }
            if(result.getValue(ratingFamily, summaryColumn) == null){
                nullSummaryCount += 1;
                //System.out.println("Summary - " + Bytes.toString(result.getValue(ratingFamily, summaryColumn)));
                //System.out.println("Null summary count incremented.");

            }

        }
        System.out.println("Null Review Count: " + nullReviewCount);
        System.out.println("Null Rating Count: " + nullRatingCount);
        System.out.println("Null Reviewer Count: " + nullReviewerIDCount);
        System.out.println("Null Summary Count: " + nullSummaryCount);
        for(Result result : metadataScanner){
            if(result.getValue(metadataFamily, titleColumn)== null){
                nullTitleCount += 1;
                //System.out.println("Title - " + Bytes.toString(result.getValue(metadataFamily, titleColumn)));
                //System.out.println("Null title count incremented.");

            }
            if(result.getValue(metadataFamily, priceColumn) == null){
                nullPriceCount += 1;
                //System.out.println("Title - " + Bytes.toString(result.getValue(metadataFamily, titleColumn)));
                //System.out.println("Null price count incremented.");

            }
            if(result.getValue(metadataFamily, brandColumn)== null){
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