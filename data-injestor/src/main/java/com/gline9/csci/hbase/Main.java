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
            findCorrelation(connection);
            //findRelationship(connection);
        }
    }

    public static void countNullValues(Connection connection) throws IOException {
        Table reviewTable = connection.getTable(TableName.valueOf("reviews"));
        Table metadataTable = connection.getTable(TableName.valueOf("metadata"));
        Scan ratingScanner = new Scan();
        Scan reviewScanner = new Scan();
        Scan reviewerIDScanner = new Scan();
        Scan summaryScanner = new Scan();
        Scan titleScanner = new Scan();
        Scan priceScanner = new Scan();
        Scan brandScanner = new Scan();

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

        ratingScanner.addColumn(ratingFamily, ratingColumn);
        reviewScanner.addColumn(ratingFamily, reviewColumn);
        reviewerIDScanner.addColumn(ratingFamily, reviewerIDColumn);
        summaryScanner.addColumn(ratingFamily, summaryColumn);
        titleScanner.addColumn(metadataFamily, titleColumn);
        priceScanner.addColumn(metadataFamily, priceColumn);
        brandScanner.addColumn(metadataFamily, brandColumn);

        ResultScanner ratingScan = reviewTable.getScanner(ratingScanner);
        ResultScanner reviewScan = reviewTable.getScanner(reviewScanner);
        ResultScanner reviewerIDScan = reviewTable.getScanner(reviewerIDScanner);
        ResultScanner summaryScan = reviewTable.getScanner(summaryScanner);
        ResultScanner titleScan = metadataTable.getScanner(titleScanner);
        ResultScanner priceScan = metadataTable.getScanner(priceScanner);
        ResultScanner brandScan = metadataTable.getScanner(brandScanner);

        int nullReviewCount = 0;
        int nullRatingCount = 0;
        int nullReviewerIDCount = 0;
        int nullSummaryCount = 0;
        int nullTitleCount = 0;
        int nullPriceCount = 0;
        int nullBrandCount = 0;


        for (Result result = reviewScan.next(); result != null; result = reviewScan.next()) {
            String review = Bytes.toString(result.getValue(ratingFamily, reviewColumn));
            if (review.equals("")) {
                nullReviewCount += 1;
            }
        }
        System.out.println(nullReviewCount); // 0

        for (Result result = ratingScan.next(); result != null; result = ratingScan.next()) {
            short rating = Bytes.toShort(result.getValue(ratingFamily, ratingColumn));
            if (rating == 0) {
                nullRatingCount += 1;
            }
        }

        System.out.println(nullRatingCount); //3
        for (Result result = reviewerIDScan.next(); result != null; result = reviewerIDScan.next()) {
            String reviewerID = Bytes.toString(result.getValue(ratingFamily, reviewerIDColumn));
            if (reviewerID.equals("")) {
                nullReviewerIDCount += 1;
            }
        }
        System.out.println(nullReviewerIDCount); //0

        for (Result result = titleScan.next(); result != null; result = titleScan.next()) {
            String title = Bytes.toString(result.getValue(metadataFamily, titleColumn));
            if (title.equals("")) {
                nullTitleCount += 1;
            }
        }
        System.out.println(nullTitleCount);//4365

        for (Result result = priceScan.next(); result != null; result = priceScan.next()) {
            double price = Bytes.toDouble(result.getValue(metadataFamily, priceColumn));
            if (price == 0) {
                nullPriceCount += 1;
            }
        }
        System.out.println(nullPriceCount); //129

        for (Result result = brandScan.next(); result != null; result = brandScan.next()) {
            String brand = Bytes.toString(result.getValue(metadataFamily, brandColumn));
            if (brand.equals("")) {
                nullBrandCount += 1;
            }
        }
        System.out.println(nullBrandCount); //1411951

        for (Result result = summaryScan.next(); result != null; result = summaryScan.next()) {
            String summary = Bytes.toString(result.getValue(metadataFamily, summaryColumn));
            if (summary.equals("")) {
                nullSummaryCount += 1;
            }
        }
        System.out.println(nullSummaryCount); //0
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

    }
}