# HiveXmlXsdSerde

The HiveXmlXsdSerde enable us to create hive table using xml schema i.e xsd file. You
can expose hive table on top of xml data by defining relevent xsd schema while creating 
external table.


## How to use 
Lets look into how to use the HiveXmlXsdSerde. Create a fat jar by running following maven command and add jar to classpath.
```
mvn clean package
```
Lets take a sample xsd file and name it as a test.xsd 
```
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
    <xs:element name="shiporder">
        <xs:complexType>
            <xs:sequence>
                <xs:element name="orderperson" type="xs:string"/>
                <xs:element name="shipto">
                    <xs:complexType>
                        <xs:sequence>
                            <xs:element name="name" type="xs:string"/>
                            <xs:element name="address" type="xs:string"/>
                            <xs:element name="city" type="xs:string"/>
                            <xs:element name="country" type="xs:string"/>
                        </xs:sequence>
                    </xs:complexType>
                </xs:element>
                <xs:element name="item" maxOccurs="unbounded">
                    <xs:complexType>
                        <xs:sequence>
                            <xs:element name="title" type="xs:string"/>
                            <xs:element name="note" type="xs:string" minOccurs="0"/>
                            <xs:element name="quantity" type="xs:positiveInteger"/>
                            <xs:element name="price" type="xs:decimal"/>
                         </xs:sequence>
                    </xs:complexType>
                </xs:element>
            </xs:sequence>
            <xs:attribute name="orderid" type="xs:string" use="required"/>
        </xs:complexType>
    </xs:element>
</xs:schema>

```

Below is the sample XML for above xsd.

```
<?xml version="1.0" encoding="UTF-8"?>
<shiporder orderid="889923"
           xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
           xsi:noNamespaceSchemaLocation="shiporder.xsd">
    <orderperson>John Smith</orderperson>
    <shipto>
        <name>Ola Nordmann</name>
        <address>Langgt 23</address>
        <city>4000 Stavanger</city>
        <country>Norway</country>
    </shipto>
    <item>
        <title>Empire Burlesque</title>
        <quantity>1</quantity>
        <price>10.90</price>
    </item>
    <item>
        <title>Hide your heart</title>
        <quantity>1</quantity>
        <price>9.90</price>
    </item>
</shiporder>

```

#### Table Creation
DDL for serde is given below. please make sure that path for schema.file.location is correct and it should be full path to xsd file.
```
add jar Hive-Xml-Xsd-Serde-1.0-SNAPSHOT-jar-with-dependencies.jar;

create external table test 
ROW FORMAT SERDE 'com.exadatum.hive.xsd.serde.XmlXsdSerDe'
WITH SERDEPROPERTIES (
"schema.file.location"="/home/exadatum/test.xsd",
)
STORED AS
INPUTFORMAT 'com.exadatum.hive.xsd.serde.readerwriter.XmlInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'
LOCATION '/user/exadatum/test'
;
```


#### Access pattern 
The record from table can be access as simillar as we access for any other table in hive for example Struct record can be accessed via dot(.) operator
```
select shipto.name from test;
```


@CopyRight Apache Liscence 2.0