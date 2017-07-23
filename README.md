# HBase TSDB Example

This repository contains an example of a time series database based on HBase. It was written to learn more about HBase in a domain that I know rather well, not for use in any product or with a realistic use case.

It contains:
- code to insert random time series data into HBase
- code to do some analytics on the series (find the maximum of the whole series)

## Terminology

A simplified model of the real time series domain is used here.

A tag is a time series with a numerical ID. A time series is an ordered list of points. Each point is a timestamp-value pair.

## High-Level Design

Everything is stored in one table.

Each point is stored as a row in HBase. Rows in HBase tables are sorted by their row keys.

The row key is defined as an 8 byte tag id, followed by an 8 byte timestamp. Two columns are stored, one timestamp (redundant...) and one double value.

Read performance is somewhat acceptable because the `setRowPrefixFilter` method of a `Scan` object seeks the right start row.

Compared to OpenTSDB, this storage model is simplified a lot. OpenTSDB stores a whole chunk of a time series in one row.

## Usage

- Start HBase
- Run `be.hoekx.hbase.main.Write` to generate and insert values
- Run `be.hoekx.hbase.main.Read` to find the maximum value in series 4

## Storage

Without compression, the example data set of 20 tags containing 4 years of data sampled at one minute intervals would take: (8 + 8) * 60 * 24 * 365 * 4 * 20 ~= 32 MB * 20 ~= 640 MB.
The example data set takes 4 GB in data archive size in HBase.

## Next Steps

- More efficient storage by reusing the timestamp of the row key
- Implement the scanning in a coprocessor to avoid data transfer overhead
