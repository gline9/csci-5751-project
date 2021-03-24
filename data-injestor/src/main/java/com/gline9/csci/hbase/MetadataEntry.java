package com.gline9.csci.hbase;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class MetadataEntry
{
    private static final Pattern PRICE_REGEX = Pattern.compile("\\$(?<min>[0-9,]+\\.\\d+)( - \\$(?<max>[0-9,]+\\.\\d+))?");
    private String asin;
    private String price;
    private String brand;
    private String title;
    private List<String> category;
    private List<String> feature;
    private List<String> also_view;
    private List<String> also_buy;

    public Put toMetadataPut()
    {
        byte[] metadataFamily = Bytes.toBytes("m");
        Put put = new Put(Bytes.toBytes(asin));
        put.addColumn(metadataFamily, Bytes.toBytes("title"), Bytes.toBytes(title));
        Double price = getPrice();
        if (null != price)
        {
            put.addColumn(metadataFamily, Bytes.toBytes("price"), Bytes.toBytes(price));
        }
        put.addColumn(metadataFamily, Bytes.toBytes("brand"), Bytes.toBytes(brand));

        appendListChoiceColumnToPut(put, Bytes.toBytes("c"), category);

        return put;
    }

    public Put toProductLinkPut()
    {
        Put put = new Put(Bytes.toBytes(asin));
        put.addColumn(Bytes.toBytes("m"), Bytes.toBytes("title"), Bytes.toBytes(title));

        appendListChoiceColumnToPut(put, Bytes.toBytes("v"), also_view);
        appendListChoiceColumnToPut(put, Bytes.toBytes("b"), also_buy);

        return put;
    }

    private void appendListChoiceColumnToPut(Put put, byte[] columnFamily, List<String> values)
    {
        for (String value : values)
        {
            put.addColumn(columnFamily, Bytes.toBytes(value), Bytes.toBytes(true));
        }
    }

    public Optional<Delete> toCategoryDelete()
    {
        Set<String> toDelete = new HashSet<>(category);
        toDelete.retainAll(feature);

        if (toDelete.isEmpty())
        {
            return Optional.empty();
        }

        Delete delete = new Delete(Bytes.toBytes(asin));
        byte[] categoryFamily = Bytes.toBytes("c");

        for (String deletion : toDelete)
        {
            delete.addColumns(categoryFamily, Bytes.toBytes(deletion));
        }

        return Optional.of(delete);
    }

    public Double getPrice()
    {
        if (null == price || price.trim().isEmpty())
        {
            return null;
        }

        Matcher matcher = PRICE_REGEX.matcher(price.trim());

        if (!matcher.matches())
        {
            // unknown format, return null
            return null;
        }

        Double min = parseDoubleWithCommas(matcher.group("min"));
        Double ret = min;
        String maybeMax = matcher.group("max");

        if (null != maybeMax)
        {
            Double max = parseDoubleWithCommas(maybeMax);
            ret = (min + max) / 2;
        }

        return ret;
    }

    private Double parseDoubleWithCommas(String string)
    {
        return Double.parseDouble(string.replaceAll(",", ""));
    }

    public String toString()
    {
        return String.format("ASIN: '%s', Price: '%.2f', Brand: '%s', Categories: '%d', Also Viewed: '%d', Also Bought: '%d'", asin, getPrice(), brand, category.size(), also_view.size(), also_buy.size());
    }

    public String getAsin()
    {
        return asin;
    }

}
