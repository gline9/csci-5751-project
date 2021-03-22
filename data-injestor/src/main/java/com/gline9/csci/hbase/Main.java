package com.gline9.csci.hbase;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
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
//            printReviewStats(connection);
            countNullValues(connection);
        }
    }
    public static void countNullValues(Connection connection) throws IOException{
        Table reviewTable = connection.getTable(TableName.valueOf("reviews"));
        Table metadataTable = connection.getTable(TableName.valueOf("metadata"))
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
        byte[] metadataFamily = Bytes.toBytes("m")
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
        ResultScanner titleScan = reviewTable.getScanner(titleScanner);
        ResultScanner priceScan = reviewTable.getScanner(priceScanner);
        ResultScanner brandScan = reviewTable.getScanner(brandScanner);

        for (Result result = reviewScan.next(); result != null; result = reviewScan.next()) {
            System.out.println(Result)
        }



    }

}