# Glink

Glink is a spatial extension of [Apache Flink](https://flink.apache.org/). Writing an 
unbounded spatial data processing program on Glink can become as simple as writing a WordCount program.

Learn more about Glink at [https://glink-incubator.github.io/](https://glink-incubator.github.io/)

## Features

Currently, Glink supports the following features:
+ Spatial Filter: filter the spatial data stream with one or more polygons.
+ Spatial Window KNN: perform KNN in the window snapshots of a spatial data stream.
+ Spatial Join
  + Spatial Dimension Join: spatial join with a spatial data stream and a spatial table.
  + Spatial Window Join: spatial window join of two spatial data streams.
  + Spatial Interval Join: spatial interval join of two spatial data streams.
+ Spatial Window DBSCAN: perform DNSCAN cluster in the window snapshots of a spatial data stream.
+ Spatial Heatmap: convert unbounded spatial data streams into heatmaps that can be displayed on web pages.

## Examples

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

SpatialDataStream<Point> pointDataStream = new SpatialDataStream<>(
        env, host, port, new SimplePointFlatMapper());
// do spatial filter on point data stream
DataStream<Point> resultStream = SpatialFilter.filter(pointDataStream, ...);
// or other spatial processings
// DataStream<Point> resultStream = SpatialWindowKNN.knn(pointDataStream, ...)
// DataStream<Point> resultStream = SpatialDimensionJoin.join(pointDataStream, otherGeometryStream, ...)
// DataStream<Point> resultStream = WindowDBSCAN.dbscan(pointDataStream, ...)
// DataStream<TileResult<Double>> heatmapStream = SpatialHeatMap.heatmap(pointDataStream, ...)
resultStream.print();
```

## Building Glink from Source

Prerequisites for building Flink:
+ Unix-like environment (we use Linux, Mac OS X, Cygwin, WSL)
+ Git
+ Maven (we recommend version 3.2.5 and require at least 3.1.1)
+ Java 8

```shell
git clone git@github.com:glink-incubator/glink.git
cd glink
mvn clean package -DskipTests
```

## Developing Glink

We recommend using IntelliJ IDEA to develop Glink and install the CheckStyle-IDEA plugin.

## Support

If you find any bugs or want to contribute new features, please open an issue and try to use 
English as much as possible. We will discuss with you as soon as possible.

If you have any questions about usage or principle, please contact liebingyu@whu.edu.cn, 
we will reply as soon as possible.

## Documentation

The documentation of Glink is located on the website: [https://glink-incubator.github.io/](https://glink-incubator.github.io/)
and the source code of the documentation is in [https://github.com/glink-incubator/glink-docs/](https://github.com/glink-incubator/glink-docs/).

## Fork and Contribute

Glink is an open-source project. We are always open to people who want to use the system or 
contribute to it.

## About

Glink was originally initiated by [Liebing](https://liebing.org.cn/).