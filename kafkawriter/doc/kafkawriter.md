# DataX KafkaWriter 插件文档


------------

## 1 快速介绍

KakfaWriter提供向kafka中指定topic写数据。


## 2 功能与限制
目前kafkaWriter支持向单个topic中写入文本类型数据或者json格式数据

## 3 功能说明


### 3.1 配置样例

```json
{  
   "job":{  
      "setting":{  
         "speed":{  
            "channel":1
         }
      },
      "content":[  
         {  
            "reader":{  
               "name":"oraclereader",
               "parameter":{  
                  "username":"zkcj",
                  "password":"zkcj2018",
                  "connection":[  
                     {  
                        "jdbcUrl":[  
                           "jdbc:oracle:thin:@10.1.20.169:1521:GYJG"
                        ],
                        "querySql":[  
                           "select * from VM_DRV_PREASIGN_A"
                        ]
                     }
                  ]
               }
            },
            "writer":{ 
		"name": "kafkawriter",
			  "parameter": {
			    "topic": "test-topic",
			    "bootstrapServers": "10.1.20.150:9092",
			    "fieldDelimiter":"\t",
			    "batchSize":10,
			     "writeType":"json",
			    "noTopicCreate":true,
			    "topicNumPartition":1,
			    "topicReplicationFactor":1
			  }
		 }
         }
      ]
   }
}
```

### 3.2 参数说明

* **bootstrapServers**

	* 描述：kafka服务地址，格式：host1:port,host2:port 样例：10.1.20.111:9092,10.1.20.121:9092<br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **topic**

	* 描述：kafka Topic 名称， 目前支持一次写入单个topic <br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **ack**

 	* 描述：消息的确认机制，默认值是0
             
      acks=0：如果设置为0，生产者不会等待kafka的响应。
      acks=1：这个配置意味着kafka会把这条消息写到本地日志文件中，但是不会等待集群中其他机器的成功响应。
      acks=all：这个配置意味着leader会等待所有的follower同步完成。这个确保消息不会丢失，除非kafka集群中所有机器挂掉。这是最强的可用性保证。
       <br />

	* 必选：否 <br />

	* 默认值：0 <br />
* **batchSize**

	* 描述：当多条消息需要发送到同一个分区时，生产者会尝试合并网络请求。这会提高client和生产者的效率 <br />
	    默认值：16384

	* 必选：否 <br />

	* 默认值：16384 <br />
* **retries**

 	* 描述：配置为大于0的值的话，客户端会在消息发送失败时重新发送： <br />

		默认值：0

	* 必选：否 <br />

	* 默认值：0 <br />

* **fieldDelimiter**

	* 描述：当wirteType为text时，写入时的字段分隔符<br />

	    默认值：,（逗号）
	* 必选：否 <br />

	* 默认值：, <br />


* **keySerializer**

	* 描述：键序列化，默认org.apache.kafka.common.serialization.StringSerializer<br />

	* 必选：否 <br />

 	* 默认值：org.apache.kafka.common.serialization.StringSerializer <br />

* **valueSerializer**

	* 描述：键序列化，默认org.apache.kafka.common.serialization.StringSerializer<br />

	* 必选：否 <br />

 	* 默认值：org.apache.kafka.common.serialization.StringSerializer <br />

* **notopicCreate**

	* 描述：当没有topic时，是否创建topic，默认false<br />

 	* 必选：haveKerberos 为true必选 <br />
 
 	* 默认值：false <br />

* **topicNumPartition**

	* 描述：topic Partition 数量<br />

 	* 必选：否 <br />
 
 	* 默认值：1 <br />

* **topicReplicationFactor**

	* 描述：topic replication 数量 <br />

 	* 必选：否 <br />
 
 	* 默认值：1 <br />
 	
* **writeType**

    * 描述：写入到kafka中的数据格式，可选text, json <br />
        text:使用fieldDelimiter拼接所有字段值作为key,value相同，然后写到kafka
        json:key和text格式相同，使用fieldDelimiter拼接所有字段值作为key，value使用datx内部column格式， 如下
        rawData为数据值，如果对象中没有该字段， 表示该值为null
        ```json
          {  
             "data":[  
                {  
                   "byteSize":13,
                   "rawData":"xxxx",
                   "type":"STRING"
                },
                {  
                   "byteSize":1,
                   "rawData":"1",
                   "type":"STRING"
                },
                {  
                   "byteSize":12,
                   "rawData":"xxx",
                   "type":"STRING"
                },
                {  
                   "byteSize":1,
                   "rawData":"A",
                   "type":"STRING"
                },
                {  
                   "byteSize":18,
                   "rawData":"xxx",
                   "type":"STRING"
                },
                {  
                   "byteSize":3,
                   "rawData":"xxx",
                   "type":"STRING"
                },
                {  
                   "byteSize":1,
                   "rawData":"A",
                   "type":"STRING"
                },
                {  
                   "byteSize":1,
                   "rawData":"0",
                   "type":"DOUBLE"
                },
                {  
                   "byteSize":8,
                   "rawData":1426740491000,
                   "subType":"DATETIME",
                   "type":"DATE"
                },
                {  
                   "byteSize":8,
                   "rawData":1426780800000,
                   "subType":"DATETIME",
                   "type":"DATE"
                },
                {  
                   "byteSize":1,
                   "rawData":"E",
                   "type":"STRING"
                },
                {  
                   "byteSize":7,
                   "rawData":"5201009",
                   "type":"STRING"
                },
                {  
                   "byteSize":6,
                   "rawData":"520101",
                   "type":"DOUBLE"
                },
                {  
                   "byteSize":0,
                   "type":"STRING"
                },
                {  
                   "byteSize":3,
                   "rawData":"xxx",
                   "type":"STRING"
                },
                {  
                   "byteSize":12,
                   "rawData":"520181000400",
                   "type":"STRING"
                },
                {  
                   "byteSize":0,
                   "type":"STRING"
                },
                {  
                   "byteSize":1,
                   "rawData":"0",
                   "type":"DOUBLE"
                },
                {  
                   "byteSize":0,
                   "subType":"DATETIME",
                   "type":"DATE"
                },
                {  
                   "byteSize":1,
                   "rawData":"0",
                   "type":"DOUBLE"
                },
                {  
                   "byteSize":0,
                   "type":"STRING"
                },
                {  
                   "byteSize":0,
                   "type":"STRING"
                },
                {  
                   "byteSize":78,
                   "rawData":"xxx",
                   "type":"STRING"
                },
                {  
                   "byteSize":1,
                   "rawData":"0",
                   "type":"STRING"
                },
                {  
                   "byteSize":8,
                   "rawData":1426694400000,
                   "subType":"DATETIME",
                   "type":"DATE"
                },
                {  
                   "byteSize":0,
                   "type":"STRING"
                },
                {  
                   "byteSize":0,
                   "subType":"DATETIME",
                   "type":"DATE"
                },
                {  
                   "byteSize":0,
                   "type":"STRING"
                },
                {  
                   "byteSize":0,
                   "type":"STRING"
                },
                {  
                   "byteSize":0,
                   "type":"STRING"
                },
                {  
                   "byteSize":0,
                   "type":"DOUBLE"
                },
                {  
                   "byteSize":1,
                   "rawData":"0",
                   "type":"STRING"
                },
                {  
                   "byteSize":0,
                   "type":"STRING"
                },
                {  
                   "byteSize":12,
                   "rawData":"520181000400",
                   "type":"STRING"
                },
                {  
                   "byteSize":1,
                   "rawData":"1",
                   "type":"DOUBLE"
                },
                {  
                   "byteSize":1,
                   "rawData":"0",
                   "type":"DOUBLE"
                },
                {  
                   "byteSize":8,
                   "rawData":1426740491000,
                   "subType":"DATETIME",
                   "type":"DATE"
                },
                {  
                   "byteSize":2,
                   "rawData":"xxx",
                   "type":"STRING"
                },
                {  
                   "byteSize":0,
                   "type":"STRING"
                },
                {  
                   "byteSize":28,
                   "rawData":"YxIC7zeM6xG+eBdzxV4oRDxHses=",
                   "type":"STRING"
                }
             ],
             "size":40
          }
        ```

    * 必选：否 <br />
 
    * 默认值：text <br />


### 3.3 类型转换

目前 HdfsWriter 支持大部分 Hive 类型，请注意检查你的类型。

下面列出 HdfsWriter 针对 Hive 数据类型转换列表:

| DataX 内部类型| HIVE 数据类型    |
| -------- | -----  |
| Long     |TINYINT,SMALLINT,INT,BIGINT |
| Double   |FLOAT,DOUBLE |
| String   |STRING,VARCHAR,CHAR |
| Boolean  |BOOLEAN |
| Date     |DATE,TIMESTAMP |


## 4 配置步骤

## 5 约束限制

略

## 6 FAQ

略
