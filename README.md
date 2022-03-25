# Data IO

### A Free based DSL for reading and writing data that supports highly used features out of the box

[Slide Deck](https://docs.google.com/presentation/d/1QptgaD6BvEKZjDBRieA1B66XtnIn9JRm8B7_FHoDvYk)

[DSL Walkthrough](https://www.youtube.com/watch?v=W5GrOrVx5W4)

## Why
We have observed, over a period of time in Data Engineering, a need for a robust API on top of the existing frameworks so that we can declaratively provide (often used) parameters to read and write Data Sources with standard set of features.

We have built SourceLoader and SinkWriter APIs which can be imported and used everywhere whenever any project needs to load/save a Data Source.

It is also very easy to add new feature to an existing framework
|Supported Frameworks|
|--------------------|
|Apache Spark|
|Apache Beam|
## Design

   1. Builder like pattern where it is easy to add or remove features related to reading/writing a data source. 
   2. Most of the features are related to three major components of an IO operation namely 
      1. Reader (Example: DataFrameReader in Spark)
      2. Writer (Example: DataFrameWriter in Spark)
      3. Data Collection (Example: DataFrame in Spark, PCollection in Beam)
   3. An interface SupportedFeatures has been built which will house all the reader/writer features.

## Supported Features
Please refer to our [Supported Features](https://github.com/zeotap/data-io/wiki/Supported-Features) guide.

## Usage
Please go through the [Wiki](https://github.com/zeotap/data-io/wiki) to understand the usage of the library.

## Build
Project is build using `sbt`

## Contributing
See the [CONTRIBUTING](/CONTRIBUTING.md) file for how to help out.
