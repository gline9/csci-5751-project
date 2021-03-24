package com.gline9.csci.hbase;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.HashSet;
import java.util.Set;

public class BrandResult
{
    private final String asin;
    private final String brandName;
    private final Set<String> categories;

    public BrandResult(Result result)
    {
        byte[] brandBytes = result.getValue(Bytes.toBytes("m"), Bytes.toBytes("brand"));
        brandName = null == brandBytes ? null : Bytes.toString(brandBytes);

        categories = new HashSet<>();
        result.getFamilyMap(Bytes.toBytes("c")).keySet().forEach(key -> categories.add(Bytes.toString(key)));
        asin = Bytes.toString(result.getRow());
    }

    public BrandResult(BrandResult a, BrandResult b)
    {
        brandName = a.brandName == null ? b.brandName : a.brandName;
        categories = new HashSet<>(a.categories);
        categories.addAll(b.categories);
        asin = a.asin;
    }

    public String getBrandName()
    {
        return brandName;
    }

    public Set<String> getCategories()
    {
        return categories;
    }

    public String toString()
    {
        return String.format("ASIN: %s, Brand: %s, Categories: %d", asin, brandName, categories.size());
    }
}
