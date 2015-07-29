# odps_sink

A plugin for Flume to provide ODPS sink (ODPS is a product of Aliyun)

## Usage

- Run `mvn clean package` to build jar
- Put the jar to lib folder of Flume, with ODPS related jar
- Modify conf file

## Roadmap

- Support minute level partition
- Configurable sink schema
- Change behavior of splitting records by `\n` to a more general pupose way

## Authors && Contributors

shaolianjie waqu
