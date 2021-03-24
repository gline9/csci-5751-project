package com.gline9.csci.hbase;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.protobuf.generated.FilterProtos;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Main
{
    public static void main(String[] args) throws IOException
    {
        loadData(args);
        performQueries(args);
    }

    private static void performQueries(String[] args) throws IOException
    {
        Configuration configuration = HBaseConfiguration.create();
        try (Connection connection = ConnectionFactory.createConnection(configuration))
        {
            queryReviewerStats(connection);
            getOrderedRecommendations(connection, args);
            getMostBoughtTogether(connection);
        }
    }

    private static void getMostBoughtTogether(Connection connection) throws IOException
    {
        Table productLinksTable = connection.getTable(TableName.valueOf("productLinks"));
        Table metadataTable = connection.getTable(TableName.valueOf("metadata"));


        Map<String, Integer> numReviewMap = new HashMap<>();
        try (ResultScanner scanner = metadataTable.getScanner(new Scan()))
        {
            int index = 0;
            for (Result result : scanner)
            {
                index++;

                if (index % 10000 == 0)
                {
                    System.out.println("First Scan Index: " + index);
                }
                String asin = Bytes.toString(result.getRow());
                if (null == asin)
                {
                    continue;
                }

                numReviewMap.put(asin, Optional.ofNullable(result.getFamilyMap(Bytes.toBytes("o"))).map(Map::size).orElse(0));
            }
        }

        int minResults = 20;
        TreeSet<ProductPair> ret = new TreeSet<>();
        try (ResultScanner scanner = productLinksTable.getScanner(new Scan()))
        {
            int index = 0;
            for (Result result : scanner)
            {
                index++;

                if (index % 10000 == 0)
                {
                    System.out.println("First Scan Index: " + index);
                }
                String asin = Bytes.toString(result.getRow());
                if (null == asin)
                {
                    continue;
                }

                List<String> bought = Optional.ofNullable(result.getFamilyMap(Bytes.toBytes("b")))
                        .orElse(Collections.emptyNavigableMap())
                        .keySet()
                        .stream()
                        .map(Bytes::toString)
                        .collect(Collectors.toList());

                List<ProductPair> productPairs = ProductPair.fromMap(asin, bought, numReviewMap);

                for (ProductPair productPair : productPairs)
                {
                    if (ret.size() < minResults)
                    {
                        ret.add(productPair);
                    }

                    ProductPair first = ret.first();
                    if (first.compareTo(productPair) < 0)
                    {
                        ret.remove(first);
                        ret.add(productPair);
                    }
                }

            }
        }

        System.out.println("****** Final results ******");

        for (ProductPair pair : ret)
        {
            System.out.println(pair.toString(metadataTable));
        }
    }

    private static Result getProductMetadata(Table metadataTable, String asin) throws IOException
    {
        return metadataTable.get(new Get(Bytes.toBytes(asin)));
    }

    private static void getOrderedRecommendations(Connection connection, String[] options) throws IOException
    {
        Table pageRankTable = connection.getTable(TableName.valueOf("pageRank"));
        Table productLinksTable = connection.getTable(TableName.valueOf("productLinks"));
        Table metadataTable = connection.getTable(TableName.valueOf("metadata"));
        Table productIndexTable = connection.getTable(TableName.valueOf("productIndex"));

        String asin;
        if (options.length > 0)
        {
            asin = options[0];
        }
        else
        {
            asin = getRandomProduct(new Random(), productIndexTable);
        }

        List<String> linkedAsins = getProductLinks(asin, productLinksTable);

        List<Get> pageRankGets = linkedAsins.stream().map(Bytes::toBytes).map(Get::new).collect(Collectors.toList());
        Result[] results = pageRankTable.get(pageRankGets);
        Map<String, PageRankRecord> pageRankRecords = Arrays.stream(results)
                .map(PageRankRecord::new)
                .collect(Collectors.toMap(PageRankRecord::getAsin, Function.identity(), PageRankRecord::merge));

        String name = getProductName(asin, metadataTable);


        List<PageRankRecord> orderedRecords = new ArrayList<>();

        for (String linkedAsin : linkedAsins)
        {
            if (pageRankRecords.containsKey(linkedAsin))
            {
                orderedRecords.add(pageRankRecords.get(linkedAsin));
            }
            else
            {
                orderedRecords.add(new PageRankRecord(linkedAsin, 0L));
            }

        }

        orderedRecords.sort(Comparator.comparing(PageRankRecord::getRank).reversed());

        System.out.printf("Recommendations for product: '%s' ASIN: '%s'%n", name, asin);

        for (PageRankRecord record : orderedRecords)
        {
            String productName = getProductName(record.getAsin(), metadataTable);
            if (null == productName)
            {
                continue;
            }

            System.out.printf("Name: '%s', Rank: '%d'%n", productName, record.getRank());
        }
    }

    private static String getProductName(String asin, Table metadataTable) throws IOException
    {
        Result nameResult = metadataTable.get(new Get(Bytes.toBytes(asin)).addColumn(Bytes.toBytes("m"), Bytes.toBytes("title")));
        return Bytes.toString(nameResult.getValue(Bytes.toBytes("m"), Bytes.toBytes("title")));
    }

    private static void createPageRank(Connection connection) throws IOException
    {
        Table productIndexTable = connection.getTable(TableName.valueOf("productIndex"));
        Table productLinks = connection.getTable(TableName.valueOf("productLinks"));
        Table pageRrankTable = connection.getTable(TableName.valueOf("pageRank"));

        int pageRankIterations = 100_000_000;
        int batchSize = 1000;
        Random random = new Random();

        String nextProduct = null;
        // handles cycles
        int maxIterationsBeforeJump = 1000;
        int iterationsSinceJump = 0;

        for (int iteration = 0; iteration < pageRankIterations; iteration++)
        {
            if (null == nextProduct || iterationsSinceJump > maxIterationsBeforeJump)
            {
                nextProduct = getRandomProduct(random, productIndexTable);
                iterationsSinceJump = 0;
            }

            incrementViewCount(nextProduct, pageRrankTable);
            iterationsSinceJump++;

            if (iteration % 1000 == 0)
            {
                printProgress(iteration, pageRankIterations);
            }

            nextProduct = nextProduct(nextProduct, productLinks, random);
        }
    }

    private static String getRandomProduct(Random random, Table productIndexTable) throws IOException
    {
        int numProducts = 15023059;
        int randomIndex = random.nextInt(numProducts) + 1;
        Get randomGet = new Get(Bytes.toBytes(randomIndex));

        Result result = productIndexTable.get(randomGet);

        return Bytes.toString(result.getValue(Bytes.toBytes("i"), Bytes.toBytes("asin")));
    }

    private static void incrementViewCount(String asin, Table pageRankTable) throws IOException
    {
        Increment increment = new Increment(Bytes.toBytes(asin));
        increment.addColumn(Bytes.toBytes("r"), Bytes.toBytes("rank"), 1);
        pageRankTable.increment(increment);
    }

    private static String nextProduct(String asin, Table productLinksTable, Random random) throws IOException
    {
        List<String> products = getProductLinks(asin, productLinksTable);

        if (products.isEmpty())
        {
            return null;
        }

        return products.get(random.nextInt(products.size()));
    }

    private static List<String> getProductLinks(String asin, Table productLinksTable) throws IOException
    {
        Get get = new Get(Bytes.toBytes(asin));
        Result result = productLinksTable.get(get);
        if (result.isEmpty())
        {
            return Collections.emptyList();
        }

        List<byte[]> products = new ArrayList<>();
        products.addAll(Optional.ofNullable(result.getFamilyMap(Bytes.toBytes("v"))).orElse(Collections.emptyNavigableMap()).keySet());
        products.addAll(Optional.ofNullable(result.getFamilyMap(Bytes.toBytes("b"))).orElse(Collections.emptyNavigableMap()).keySet());

        return products.stream().map(Bytes::toString).collect(Collectors.toList());
    }

    private static void putProductIndexes(Connection connection, String metadataFileName) throws IOException
    {
        ObjectMapper objectMapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        File file = new File(metadataFileName);
        BufferedReader reader = Files.newBufferedReader(file.toPath());

        Table productIndexTable = connection.getTable(TableName.valueOf("productIndex"));

        int lines = 0;
        List<Put> productLinkPuts = new ArrayList<>();
        while (reader.ready())
        {
            lines++;
            String line = reader.readLine();

            MetadataEntry product = objectMapper.readValue(line, MetadataEntry.class);
            Put put = new Put(Bytes.toBytes(lines));
            put.addColumn(Bytes.toBytes("i"), Bytes.toBytes("asin"), Bytes.toBytes(product.getAsin()));
            productLinkPuts.add(put);

            if (lines % 1000 == 0)
            {
                printReviewProgress(lines);
            }

            if (productLinkPuts.size() >= 10000)
            {
                productIndexTable.put(productLinkPuts);
                productLinkPuts.clear();
                System.out.println("Processed metadata batch: Line " + lines);
            }

        }

        productIndexTable.put(productLinkPuts);
        productLinkPuts.clear();
        System.out.println("Processed metadata batch: Line " + lines);
    }

    private static void initializePageRankTable(Connection connection) throws IOException
    {
        Admin admin = connection.getAdmin();
        TableDescriptor pageRankTableDescriptor = TableDescriptorBuilder.newBuilder(TableName.valueOf("pageRank"))
                .setColumnFamily(ColumnFamilyDescriptorBuilder.of("r")).build();
        admin.createTable(pageRankTableDescriptor);

        Table pageRankTable = connection.getTable(TableName.valueOf("pageRank"));
    }

    private static void queryReviewerStats(Connection connection) throws IOException
    {
        Table table = connection.getTable(TableName.valueOf("overallReviews"));

        Comparator<Reviewer> minComparator = Reviewer.getMiningComparator();
        Comparator<Reviewer> maxComparator = Reviewer.getMaxingComparator();
        TreeSet<Reviewer> minReviewers = new TreeSet<>(minComparator);
        TreeSet<Reviewer> maxReviewers = new TreeSet<>(maxComparator);

        int numToGrab = 20;
        int index = 0;

        try (ResultScanner scanner = table.getScanner(new Scan()))
        {
            for (Result result : scanner)
            {
                index++;
                if (index % 10000 == 0)
                {
                    System.out.println("Index " + index);
                }

                Reviewer reviewer = new Reviewer(result);

                if (minReviewers.size() < numToGrab)
                {
                    minReviewers.add(reviewer);
                    maxReviewers.add(reviewer);
                    continue;
                }

                Reviewer maxMin = minReviewers.last();

                if (minComparator.compare(maxMin, reviewer) > 0)
                {
                    minReviewers.remove(maxMin);
                    minReviewers.add(reviewer);
                }

                Reviewer minMax = maxReviewers.first();

                if (maxComparator.compare(minMax, reviewer) < 0)
                {
                    maxReviewers.remove(minMax);
                    maxReviewers.add(reviewer);
                }
            }
        }

        System.out.println("**** Lowest Rating Reviewers ****");

        for (Reviewer reviewer : minReviewers)
        {
            System.out.println(reviewer);
        }

        System.out.println("**** Highest Rating Reviewers ****");

        for (Reviewer reviewer : maxReviewers)
        {
            System.out.println(reviewer);
        }
    }

    private static void loadData(String[] args) throws IOException
    {
        Configuration configuration = HBaseConfiguration.create();
        try (Connection connection = ConnectionFactory.createConnection(configuration))
        {
            createTables(connection);
            putMetadata(connection, args[0]);
            deleteExtraCategories(connection, args[0]);
            putReviews(connection, args[1]);
            putBrandReviews(connection, args[1]);
            putProductIndexes(connection, args[0]);
            createPageRank(connection);
        }
    }

    private static void putBrandReviews(Connection connection, String reviewFileName) throws IOException
    {
        ObjectMapper objectMapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        File file = new File(reviewFileName);
        BufferedReader reader = Files.newBufferedReader(file.toPath());

        Table brandReviewTable = connection.getTable(TableName.valueOf("categoryReviews"));
        Table metadataTable = connection.getTable(TableName.valueOf("metadata"));

        int lines = 0;
        List<Get> metadataGets = new ArrayList<>();
        List<Review> reviewPuts = new ArrayList<>();
        while (reader.ready())
        {
            lines++;
            String line = reader.readLine();

            Review review = objectMapper.readValue(line, Review.class);
            metadataGets.add(review.toProductGet());
            reviewPuts.add(review);

            if (lines % 1000 == 0)
            {
                printReviewProgress(lines);
            }

            if (reviewPuts.size() >= 10000)
            {
                processBrandReviewGets(reviewPuts, metadataGets, metadataTable, brandReviewTable);
                System.out.println("Processed review batch: Line " + lines);
            }

        }

        processBrandReviewGets(reviewPuts, metadataGets, metadataTable, brandReviewTable);
        System.out.println("Processed review batch: Line " + lines);
    }

    private static void processBrandReviewGets(List<Review> reviews, List<Get> metadataGets, Table metadataTable, Table brandReviewTable) throws IOException
    {
        if (metadataGets.isEmpty())
        {
            return;
        }

        Result[] results = metadataTable.get(metadataGets);
        Map<String, Optional<BrandResult>> resultMap = Arrays.stream(results)
                .filter(r -> null != r.getRow())
                .collect(
                        Collectors.groupingBy(
                                (Result result) -> Bytes.toString(result.getRow()),
                                Collectors.mapping(BrandResult::new,
                                        Collectors.reducing(BrandResult::new))));

        List<Put> brandReviewPuts = reviews.stream()
                .map(r -> r.toBrandReviewPuts(resultMap))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .flatMap(List::stream)
                .collect(Collectors.toList());

        brandReviewTable.put(brandReviewPuts);

        reviews.clear();
        metadataGets.clear();
    }

    private static void deleteExtraCategories(Connection connection, String metadataFileName) throws IOException
    {
        ObjectMapper objectMapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        File file = new File(metadataFileName);
        BufferedReader reader = Files.newBufferedReader(file.toPath());

        Table metadataTable = connection.getTable(TableName.valueOf("metadata"));

        int lines = 0;
        List<Delete> categoryDeletes = new ArrayList<>();
        while (reader.ready())
        {
            lines++;
            String line = reader.readLine();
            MetadataEntry metadataEntry = objectMapper.readValue(line, MetadataEntry.class);
            metadataEntry.toCategoryDelete().ifPresent(categoryDeletes::add);

            if (lines % 1000 == 0)
            {
                printMetadataProgress(lines);
            }

            if (categoryDeletes.size() >= 10000)
            {
                metadataTable.delete(categoryDeletes);
                categoryDeletes.clear();
                System.out.println("Processed metadata batch: Line " + lines);
            }

        }

        metadataTable.delete(categoryDeletes);
        categoryDeletes.clear();
        System.out.println("Processed metadata batch: Line " + lines);

        System.out.println("Done!");
    }

    private static void putReviews(Connection connection, String reviewFileName) throws IOException
    {
        ObjectMapper objectMapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        File file = new File(reviewFileName);
        BufferedReader reader = Files.newBufferedReader(file.toPath());

        Table reviewTable = connection.getTable(TableName.valueOf("reviews"));
        Table metadataTable = connection.getTable(TableName.valueOf("metadata"));
        Table overallReviewTable = connection.getTable(TableName.valueOf("overallReviews"));


        int lines = 0;
        List<Put> reviewPuts = new ArrayList<>();
        List<Put> metadataPuts = new ArrayList<>();
        List<Put> overallReivewPuts = new ArrayList<>();
        while (reader.ready())
        {
            lines++;
            String line = reader.readLine();
            if (lines <= 100)
            {
                continue;
            }
            Review review = objectMapper.readValue(line, Review.class);
            reviewPuts.add(review.toReviewPut());
            metadataPuts.add(review.toMetadataPut());
            overallReivewPuts.add(review.toOverallReviewPut());

            if (lines % 1000 == 0)
            {
                printReviewProgress(lines);
            }

            if (reviewPuts.size() >= 10000)
            {
                reviewTable.put(reviewPuts);
                reviewPuts.clear();
                System.out.println("Processed review batch: Line " + lines);
            }

            if (metadataPuts.size() >= 10000)
            {
                metadataTable.put(metadataPuts);
                metadataPuts.clear();
                System.out.println("Processed metadata batch: Line " + lines);
            }

            if (overallReivewPuts.size() >= 10000)
            {
                overallReviewTable.put(overallReivewPuts);
                overallReivewPuts.clear();
                System.out.println("Processed overall reviews batch: Line " + lines);
            }
        }

        reviewTable.put(reviewPuts);
        System.out.println("Processed review batch: Line " + lines);

        metadataTable.put(metadataPuts);
        System.out.println("Processed review batch: Line " + lines);

        overallReviewTable.put(overallReivewPuts);
        System.out.println("Processed overall reviews batch: Line " + lines);
    }

    private static void printReviewProgress(int iterations)
    {
        printProgress(iterations, 157260920);
    }

    private static void putMetadata(Connection connection, String metadataFileName) throws IOException
    {
        ObjectMapper objectMapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        File file = new File(metadataFileName);
        BufferedReader reader = Files.newBufferedReader(file.toPath());

        Table metadataTable = connection.getTable(TableName.valueOf("metadata"));
        Table productLinksTable = connection.getTable(TableName.valueOf("productLinks"));

        int lines = 0;
        List<Put> metadataPuts = new ArrayList<>();
        List<Put> productLinkPuts = new ArrayList<>();
        while (reader.ready())
        {
            lines++;
            String line = reader.readLine();
            MetadataEntry metadataEntry = objectMapper.readValue(line, MetadataEntry.class);
            metadataPuts.add(metadataEntry.toMetadataPut());
            productLinkPuts.add(metadataEntry.toProductLinkPut());

            if (lines % 1000 == 0)
            {
                printMetadataProgress(lines);
            }

            if (metadataPuts.size() >= 10000)
            {
                metadataTable.put(metadataPuts);
                metadataPuts.clear();
                System.out.println("Processed metadata batch: Line " + lines);
            }

            if (productLinkPuts.size() >= 10000)
            {
                productLinksTable.put(productLinkPuts);
                productLinkPuts.clear();
                System.out.println("Processed product links batch: Line " + lines);
            }
        }

        metadataTable.put(metadataPuts);
        metadataPuts.clear();
        System.out.println("Processed metadata batch: Line " + lines);

        productLinksTable.put(productLinkPuts);
        productLinkPuts.clear();
        System.out.println("Processed product links batch: Line " + lines);

        System.out.println("Done!");
    }

    private static void printMetadataProgress(int iterations)
    {
        printProgress(iterations, 15023059);
    }

    private static void printProgress(int iterations, int totalIterations)
    {
        double percentDone = iterations / (double)totalIterations;

        int barLength = 40;
        int barProgress = (int)(percentDone * barLength);
        int barRemaining = barLength - barProgress;
        String barCharacters = new String(new char[barProgress]).replace("\0", "#");
        String barFillCharacters = new String(new char[barRemaining]).replace("\0", ".");

        System.out.printf("[%s%s] %.2f%% (%d/%d)%n", barCharacters, barFillCharacters, percentDone * 100, iterations, totalIterations);
    }

    private static void createTables(Connection connection) throws IOException
    {
        Admin admin = connection.getAdmin();

        TableDescriptor reviewsTable = TableDescriptorBuilder.newBuilder(TableName.valueOf("reviews"))
                .setColumnFamily(ColumnFamilyDescriptorBuilder.of("r")).build();

        admin.createTable(reviewsTable);

        TableDescriptor metadataTable = TableDescriptorBuilder.newBuilder(TableName.valueOf("metadata"))
                .setColumnFamily(ColumnFamilyDescriptorBuilder.of("m"))
                .setColumnFamily(ColumnFamilyDescriptorBuilder.of("c"))
                .setColumnFamily(ColumnFamilyDescriptorBuilder.of("o")).build();

        admin.createTable(metadataTable);

        TableDescriptor productLinksTable = TableDescriptorBuilder.newBuilder(TableName.valueOf("productLinks"))
                .setColumnFamily(ColumnFamilyDescriptorBuilder.of("m"))
                .setColumnFamily(ColumnFamilyDescriptorBuilder.of("v"))
                .setColumnFamily(ColumnFamilyDescriptorBuilder.of("b")).build();

        admin.createTable(productLinksTable);

        TableDescriptor overallReviewsTable = TableDescriptorBuilder.newBuilder(TableName.valueOf("overallReviews"))
                .setColumnFamily(ColumnFamilyDescriptorBuilder.of("o")).build();

        admin.createTable(overallReviewsTable);

        TableDescriptor categoryReviews = TableDescriptorBuilder.newBuilder(TableName.valueOf("categoryReviews"))
                .setColumnFamily(ColumnFamilyDescriptorBuilder.of("o")).build();

        admin.createTable(categoryReviews);
    }
}
