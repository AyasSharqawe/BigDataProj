
This project implements a pipeline for processing tweets, extracting metadata, and integrating with Apache Kafka, Elasticsearch, and Spark. It provides an HTTP endpoint to receive tweets, processes them to extract metadata such as hashtags, geospatial data, and sentiment analysis, and sends the processed data to Kafka and Elasticsearch.

----------

## Table of Contents
1. [Features](#features)
2. [Technologies Used](#technologies-used)
3. [Getting Started](#Started)
4. [API Endpoints](#api-endpoints)
----------

## Features
- Extract hashtags from tweets.
- Analyze geospatial data (latitude and longitude).
- Perform sentiment analysis using a pretrained NLP model.
- Integrate with Kafka for streaming data.
- Store processed tweets in Elasticsearch for search and analytics.
- Built-in Spark-based NLP pipeline for data transformation.

----------

## Technologies Used
- **Scala**
- **Apache Kafka**
- **Elasticsearch** (via `elastic4s` library)
- **Apache Spark** (with NLP libraries from John Snow Labs)
- **Akka HTTP** for REST API
- **JSON** for data handling and serialization

----------

## Started
### Prerequisites
- Java (JDK 8 or higher)
- Scala (2.12.x )
- Apache Kafka
- Elasticsearch
- Spark
- sbt (Scala Build Tool)

Key configurations for the project are set in the code:
Elasticsearch: The URL in TwitterKafkaProducer : 
- val elasticsearchUrl = "http://localhost:9200"
Kafka: URL in TwitterKafkaProducer:
- val kafkaBroker = "localhost:9092"
- val kafkaTopic = "tweet-stream"

### API Endpoints
/tweets (POST)
Accepts a JSON payload representing a tweet.
Processes the tweet and stores it in Elasticsearc

Pipeline Components
1. NLP Pipeline:
  - Tokenization
  - Word Embeddings (GloVe)
  - Named Entity Recognition (NER)
  - Sentiment Analysis (using Vivekn model)
2. Metadata Extraction:
  - Hashtags: Extracted using regular expressions.
  - Geospatial Data: Extracted from JSON using custom logic.
3. Data Storage:
  - Processed tweets are sent to Kafka.
  - Stored in Elasticsearch for further analysis.



## How It Works
The project processes tweets in real-time, enriches the data with metadata such as sentiment
analysis, hashtags, and geospatial information, and integrates with Apache Kafka and Elasticsearch
for data storage and streaming. Below is a step-by-step explanation of the workflow: 

1. Tweet Ingestion
  - Tweets are ingested via a REST API endpoint (/tweets).
  - The endpoint accepts JSON payloads representing individual tweets.
  - Each tweet contains fields like created_at, text, user, and optional fields like hashtags and geo coordinates.
2. Metadata Extraction
- Hashtag Extraction :
  - Extracts hashtags using a regex pattern.
  - Example: #New is extracted from the text "Happy #New Year".
  Geospatial Data:
  - Extracts latitude and longitude from the geo field if present.
  - Stores this data as a custom Space object.
 Sentiment Analysis:
  - Analyzes the sentiment of the tweet text using a pretrained NLP pipeline (Vivekn Sentiment Model).
3. NLP Pipeline:
- Built using Spark NLP:
  - Tokenizes the text into individual words.
  - Applies GloVe word embeddings to enhance understanding.
  - Identifies named entities (NER) such as locations, organizations, and persons.
- The pipeline enriches the tweet with structured metadata.
4. Data Transformation:
   - The tweet, along with its extracted metadata, is transformed into a standardized structure.
   - This includes fields for sentiment, hashtags, geospatial data, and user information.
5. Data Storage:
- Apache Kafka:
   - The enriched tweet is serialized into JSON and sent to a Kafka topic (tweet-stream).
   - Kafka enables real-time streaming of tweets for downstream consumers.
- Elasticsearch:
  - The enriched tweet is indexed into Elasticsearch for search and analytics.
  - Example: Tweets can be queried by hashtags, sentiment, or user information.
6. File-Based Batch Processing:
  - Tweets can also be read from a JSON file (boulder_flood_geolocated_tweets.json).
  - The file's contents are processed in batches, extracting metadata and sending the results to Kafka and Elasticsearch.
7. HTTP Server:
  - The project includes an Akka HTTP server that listens on port 8080.
  - This server exposes the /tweets endpoint and manages incoming requests for tweet ingestion.
