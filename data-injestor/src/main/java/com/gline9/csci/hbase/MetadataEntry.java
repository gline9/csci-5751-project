package com.gline9.csci.hbase;

public class MetadataEntry
{
    private String asin;
    private String price;
    private String brand;

    public String toString()
    {
        return String.format("ASIN: '%s', Price: '%s', Brand: '%s'");
    }
}
