#!/bin/bash
sudo apt-get update -y
sudo apt-get install -y docker.io
sudo docker pull docker.elastic.co/elasticsearch/elasticsearch:7.6.2
sudo docker run -d --name elasticsearch -p 9200:9200 -p 9300:9300 -e "discovery.type=single-node" docker.elastic.co/elasticsearch/elasticsearch:7.6.2

sudo docker cp elasticsearch elasticsearch:/usr/share/elasticsearch/logs/elasticsearch.yaml

curl -X PUT "localhost:9200/thomreuters?pretty" -H 'Content-Type: application/json' -d'
{
    "settings": {
        "index": {
            "number_of_shards": 9
        },
        "analysis": {
            "analyzer": {
                "analyzer-name": {
                    "type": "custom",
                    "tokenizer": "keyword",
                    "filter": "lowercase"
                }
            }
        }
    },
    "mappings": {
        "properties": {
            "dates": {
                "type":   "date",
                "format": "HH:mm:ss"
            },
            "unique_story_index": {
                "type": "keyword",
                "null_value": "NULL"
            },
            "event_type": {
                "type": "keyword",
                "null_value": "NULL"
            },
            "pnac": {
                "type": "keyword",
                "null_value": "NULL"
            },
            "story_date_time": {
                "type":   "date",
                "format": "yyyy-MM-dd HH:mm:ss",
                "null_value": "NULL"
            },
            "take_date_time": {
                "type":   "date",
                "format": "yyyy-MM-dd HH:mm:ss",
                "null_value": "NULL"
            },
            "headline": {
                "type": "text",
                "analyzer": "analyzer-name"
            },
            "accumulated_story_text": {
                "type": "text"
            },
            "take_text": {
                "type": "text",
                "analyzer": "analyzer-name"
            },
            "products": {
                "type": "keyword",
                "null_value": "NULL"
            },
            "topics": {
                "type": "text",
                "analyzer": "analyzer-name"
            },
            "related_rics": {
                "type": "keyword",
                "null_value": "NULL"
            },
            "named_items": {
                "type": "keyword",
                "null_value": "NULL"
            },
            "story_type": {
                "type": "keyword",
                "null_value": "NULL"
            },
            "language": {
                "type": "keyword",
                "null_value": "NULL"
            }
        }
    }
}
'
