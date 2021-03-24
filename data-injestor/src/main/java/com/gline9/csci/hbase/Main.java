package com.gline9.csci.hbase;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.FileWriter;
import javax.swing.*;
import org.math.plot.*;
import java.awt.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;

public class Main {
    public static void main(String[] args) throws IOException {

        Configuration configuration = HBaseConfiguration.create();
        try (Connection connection = ConnectionFactory.createConnection(configuration)) {
            //countNullValues(connection);
            //findCorrelation(connection);
            //findRelationship(connection);
            ArrayList<Double> l1 = new ArrayList<>();
            ArrayList<Double> l2 = new ArrayList<>();
            l1.add(1.0);
            l2.add(2.0);
            l1.add(3.0);
            l2.add(4.0);
            l1.add(5.0);
            l2.add(6.0);
            l1.add(7.0);
            l2.add(8.0);
            l1.add(9.0);
            l2.add(10.0);
            l1.add(11.0);
            l2.add(12.0);
            writeTotxtFile(l1,l2);
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
        Table reviewTable = connection.getTable(TableName.valueOf("reviews"));

        Scan metadataScan = new Scan();
        Scan reviewScan = new Scan();
        byte[] metadataFamily = Bytes.toBytes("m");
        byte[] overallFamily = Bytes.toBytes("o");
        byte[] priceColumn = Bytes.toBytes("price");
        byte[] reviewerIDColumn = Bytes.toBytes("reviewerID");

        //metadataScan.addColumn(metadataFamily, priceColumn);
        //metadataScan.addColumn(overallFamily,reviewerIDColumn);

        ResultScanner metadataScanner = metadataTable.getScanner(metadataScan);
        ResultScanner reviewScanner = reviewTable.getScanner(reviewScan);
        double price = 0;
        short overall = 0;
        double count = 0;
        ArrayList<Double> priceList = new ArrayList<Double>();
        ArrayList<Double> overallList = new ArrayList<Double>();

        String previousKey = "";
        boolean flag = false;
        for (Result result : metadataScanner) {
            NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(overallFamily);
            for (Map.Entry<byte[], byte[]> entry : familyMap.entrySet()) {
                if (result.getValue(metadataFamily, priceColumn) != null && result.getValue(overallFamily, entry.getKey()) != null) {
                    previousKey = Bytes.toString(result.getRow());
                    flag = true;
                }
                if(flag == true){
                    break;
                }
            }
            if(flag == true){
                break;
            }
        }
        metadataScanner = metadataTable.getScanner(metadataScan);

        for (Result result : metadataScanner) {
            NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(overallFamily);
            String metadataKey = Bytes.toString(result.getRow());
            for (Map.Entry<byte[], byte[]> entry : familyMap.entrySet()) {
                if (result.getValue(metadataFamily, priceColumn) != null && result.getValue(overallFamily, entry.getKey()) != null) {
                    metadataKey = Bytes.toString(result.getRow());
                    if (previousKey.equals(metadataKey)) {
                        price = Bytes.toDouble(result.getValue(metadataFamily, priceColumn));
                        overall += (double)Bytes.toShort(result.getValue(overallFamily, entry.getKey()));
                        count += 1.0;
                    } else {
                        previousKey = metadataKey;
                        priceList.add(price);
                        overallList.add((double)overall/count);
                        count = 1.0;
                        overall = Bytes.toShort(result.getValue(overallFamily, entry.getKey()));
                        price = Bytes.toDouble(result.getValue(metadataFamily, priceColumn));
                    }
                }
            }
        }
    }
    public static void writeTotxtFile(ArrayList<Double> l1, ArrayList<Double> l2) throws IOException {
        FileWriter myWriter = new FileWriter("filename.txt");
        for(int i = 0; i < l1.size(); i++){
            myWriter.write(String.format("%f,%f", l1.get(i),l2.get(i)));
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

        //Metadata dataset metadata column family and columns
        byte[] metadataFamily = Bytes.toBytes("m");

        byte[] titleColumn = Bytes.toBytes("title");
        byte[] priceColumn = Bytes.toBytes("price");
        byte[] brandColumn = Bytes.toBytes("brand");

        int nullReviewCount = 0;
        int nullRatingCount = 0;
        int nullSummaryCount = 0;
        int nullTitleCount = 0;
        int nullPriceCount = 0;
        int nullBrandCount = 0;
        int nullReviewRatingCount = 0;
        int nullReviewSummaryCount = 0;
        int nullRatingSummaryCount = 0;
        int nullReviewRatingSummaryCount = 0;
        int nullTitlePriceCount = 0;
        int nullTitleBrandCount = 0;
        int nullPriceBrandCount = 0;
        int nullTitleBrandPriceCount = 0;

        ResultScanner reviewScanner = reviewTable.getScanner(reviewScan);
        ResultScanner metadataScanner = metadataTable.getScanner(metadataScan);

        for (Result result : reviewScanner) {
            if(result.getValue(ratingFamily, ratingColumn) == null){
                nullRatingCount += 1;
            }
            if(result.getValue(ratingFamily, reviewColumn) == null){
                nullReviewCount += 1;
            }
            if(result.getValue(ratingFamily, summaryColumn) == null){
                nullSummaryCount += 1;
            }
            if(result.getValue(ratingFamily, ratingColumn) == null && result.getValue(ratingFamily, reviewColumn) == null){
                nullReviewRatingCount += 1;
            }
            if(result.getValue(ratingFamily, ratingColumn) == null && result.getValue(ratingFamily, summaryColumn) == null){
                nullRatingSummaryCount += 1;
            }
            if(result.getValue(ratingFamily, summaryColumn) == null && result.getValue(ratingFamily, reviewColumn) == null){
                nullReviewSummaryCount += 1;
            }
            if(result.getValue(ratingFamily, summaryColumn) == null && result.getValue(ratingFamily, reviewColumn) == null && result.getValue(ratingFamily, ratingColumn) == null ){
                nullReviewSummaryCount += 1;
            }
        }

        for(Result result : metadataScanner){
            if(result.getValue(metadataFamily, titleColumn)== null){
                nullTitleCount += 1;
            }
            if(result.getValue(metadataFamily, priceColumn) == null){
                nullPriceCount += 1;
            }
            if(result.getValue(metadataFamily, brandColumn)== null){
                nullBrandCount += 1;
            }
            if(result.getValue(metadataFamily, titleColumn)== null && result.getValue(metadataFamily, priceColumn) == null ){
                nullTitlePriceCount += 1;
            }
            if(result.getValue(metadataFamily, titleColumn)== null && result.getValue(metadataFamily, brandColumn) == null ){
                nullTitleBrandCount += 1;
            }
            if(result.getValue(metadataFamily, brandColumn)== null && result.getValue(metadataFamily, priceColumn) == null ){
                nullPriceBrandCount += 1;
            }
            if(result.getValue(metadataFamily, brandColumn)== null && result.getValue(metadataFamily, priceColumn) == null && result.getValue(metadataFamily, titleColumn)== null){
                nullTitleBrandPriceCount += 1;
            }
        }


        System.out.println("     Printing stats of single null count");
        System.out.println("---------------------------------------");
        System.out.println("Null Review Count: " + nullReviewCount);
        System.out.println("Null Rating Count: " + nullRatingCount);
        System.out.println("Null Summary Count: " + nullSummaryCount);
        System.out.println("Null Title Count: " + nullTitleCount);
        System.out.println("Null Price Count: " + nullPriceCount);
        System.out.println("Null Brand Count: " + nullBrandCount);
        System.out.println("---------------------------------------");
        System.out.println("     Printing stats of pair null count");
        System.out.println("Null Review + Rating Count: " + nullReviewRatingCount);
        System.out.println("Null Review + Summary Count: " + nullReviewSummaryCount);
        System.out.println("Null Rating + Summary Count: " + nullRatingSummaryCount);
        System.out.println("Null Title + Price Count: " + nullTitlePriceCount);
        System.out.println("Null Title + Brand Count: " + nullTitleBrandCount);
        System.out.println("Null Brand + Price Count: " + nullPriceBrandCount);
        System.out.println("---------------------------------------");
        System.out.println("     Printing stats of three item null count");
        System.out.println("Null Review + Rating + Summary Count: " + nullReviewRatingSummaryCount);
        System.out.println("Null Title + Brand + Price Count: " + nullTitleBrandPriceCount);
        System.out.println("---------------------------------------");

    }

}