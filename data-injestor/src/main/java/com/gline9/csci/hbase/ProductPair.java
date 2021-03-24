package com.gline9.csci.hbase;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class ProductPair implements Comparable<ProductPair>
{
    private final String firstAsin;
    private final String secondAsin;
    private final Integer maximumPurchases;

    public ProductPair(String firstAsin, String secondAsin, Integer maximumPurchases)
    {
        this.firstAsin = firstAsin;
        this.secondAsin = secondAsin;
        this.maximumPurchases = maximumPurchases;
    }

    public static List<ProductPair> fromMap(String asin, List<String> asins, Map<String, Integer> reviewMap)
    {
        if (!reviewMap.containsKey(asin))
        {
            return Collections.emptyList();
        }

        int reviews = reviewMap.get(asin);

        return asins.stream()
                .filter(reviewMap::containsKey)
                .map(secondAsin -> new ProductPair(asin, secondAsin, Math.min(reviews, reviewMap.get(secondAsin))))
                .collect(Collectors.toList());
    }

    @Override
    public int compareTo(ProductPair o)
    {
        if (null == o.firstAsin || null == o.secondAsin)
        {
            return 1;
        }

        if (null == firstAsin || null == secondAsin)
        {
            return -1;
        }

        int comparison = Integer.compare(maximumPurchases, o.maximumPurchases);

        if (comparison != 0)
        {
            return comparison;
        }

        comparison = firstAsin.compareTo(o.firstAsin);

        if (comparison != 0)
        {
            return comparison;
        }

        return secondAsin.compareTo(o.secondAsin);
    }

    public String toString(Table metadataTable) throws IOException
    {
        String firstTitle = Bytes.toString(metadataTable.get(new Get(Bytes.toBytes(firstAsin))).getValue(Bytes.toBytes("m"), Bytes.toBytes("title")));
        String secondTitle = Bytes.toString(metadataTable.get(new Get(Bytes.toBytes(secondAsin))).getValue(Bytes.toBytes("m"), Bytes.toBytes("title")));

        return String.format("First Product: '%s'%nSecond Product: '%s'%nMaximum Purchases: '%d'", firstTitle, secondTitle, maximumPurchases);
    }
}
