# DBLoader

DBLoader is spark based framework to read structured data from RDBMS,Snowflake and S3 writes back to RDBMS or Snowflake 

Application Properties
----------------------
Appliaction properties to process data from DBLoader

```properties

#Source Options
sourceDatabase=
sourceDatabase|sourceSfDatabase(only with snowflake)=
sourceSchema|sourcetSfSchema(only with snowflake)=
sourceWarehouse|sourcetSfWarehouse(only with snowflake)=

#reader Objects
sourceTable=
sourceTableFunction=
sqlFile=

#Other reader Options
readerFormat=
readerOptions=
srcFormats=
srcToTgtColMap=
extractFilter=
hashedKeyColumns=
hashedColumn=
hashTargetOrderQualifier=

#Order of Precedence for input reads (left has highest)
#sqlFile -> sourceTableFunction -> sourceTable

#sourceTableFunction=someTableFunction(#taskExctnId#)  -- taskExctnId will be substituted at run time .
#extractFilter=extract_date=#extract_date#
#hashedKeyColumns - keys to compare
#hashedColumn - defaulted to row_hash as hashedColumn
#targetOrderColumn
#hashTargetOrderQualifier - is the value to sort(order by) the target keys and extract the latest column example load_date desc or extract_date etc..


#Target Options
writeMode=
writerFormat=
#Incase of JDBC/RDBMS Writer

targetTable|targetSfTable(only with Snowflake)=
targetDatabase|targetSfDatabase(only with snowflake)=
targetSchema|targetSfSchema(only with snowflake)=
targetWarehouse|targetSfWarehouse(only with snowflake)=

#inferTargetSchema - inferes to target Schema in case if there are more columns are present in source dataframe and need to remove some from source.
#tgtDataTypeMap - target data type conversion in case if need to update table name

#Encrytion Options
encryptedConfig=
encryptedConfigKey=
```


Where can I get the latest release?
-----------------------------------
You can get source from [SCM](https://github.com/bkosaraju/dataextractor).

Alternatively you can pull binaries from the central Maven repositories(yet to publish):
For mvn:
```xml
<dependency>
  <groupId>io.github.bkosaraju</groupId>
  <artifactId>dbloader_#ScalaVariant#</artifactId>
  <version>#Version#</version>
</dependency>
 
<!--Fat/ Assembly Jar-->
<dependency>
  <groupId>io.github.bkosaraju</groupId>
  <artifactId>dbloader_#ScalaVariant#</artifactId>
  <version>#verion#</version>
  <classifier>all</classifier>
</dependency>

```
for Gradle:

```groovy
    api group: "io.github.bkosaraju", name: "dbloader_$scalaVariant", version: "$Version"
```

## Build Instructions

```bash
./gradlew clean build

#Artifacts can be found in build/lib directory 

#Linux/Windows binaries can be found at build/distribution directory 
```

## Scala [Docs](https://bkosaraju.github.io/dbloader)

## Contributing
Please feel free to raise a pull request in case if you feel like something can be updated or contributed

## License
[Apache](http://www.apache.org/licenses/LICENSE-2.0.txt)