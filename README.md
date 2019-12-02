# Optimising Query Performance

This repository provides the implementation of all the experiments for the case studies described in our paper entitled "Optimising Query Performance in Data Streaming Applications" [1].

# Case Studies

## Amazon ##

Consider a simplified version of Amazon ordering service to identify some situations of interest:

* Q1. ProductPopularity: considering a specific product (for example product with *idProduct = 'product 10'*), get the customers that have ordered that product. With this query, we pretend to obtain the popularity of a product inside the Amazon ordering network. This query is implemented as a simple query and conditional query.

Gremlin query for simple implementation can be viewed following:

```
graph.traversal().V().as("user").out("orders").out("contains")
.has("idProduct", "product 10").select("user").dedup().toList();
```

Gremlin query for conditional implementation can be viewed following:

```
graph.traversal().V()
.where(__.out("orders").out("contains").has("idProduct", "product 10")).dedup().toList();
```
* Q2. AlternativeCustomer: given a featured event, for example the Olympic Games, and a list of products that are known to be more frequently ordered than other products, obtain the customers that do not have any order that contains these products. This query can be useful to refine the advertisement campaigns in order to increase their success, recommending its products to the customers that do not ordered them.

Gremlin query can be viewed following:

```
graph.traversal().V().hasLabel("Customer").as("user")
.not(__.out("orders").out("contains").has("idProduct", P.within(idProducts))).select("user").dedup().toList();
```

* Q3. PackagePopularity: considering two products (for example products with *idProduct = 'product 10'* and *idProduct = 'product 20'*), get the customers that have ordered both products. With this query, we obtain information about the frequency that a customer orders  two specific products. This information can be useful to create recommendations to the customers that have ordered one of these products.

Gremlin query can be viewed following:

```
graph.traversal().V()
.and(__.out("orders").out("contains").has("idProduct", "product 10"),
__.out("orders").out("contains").has("idProduct", "product 20")).dedup().toList();
```
* Q4. SimilarProductsPopularity: given two specific products that are known to be similar (for example two types of sports socks), get the customers that have ordered one of these products. This query is useful to know the popularity of products with common attributes.

Gremlin query can be viewed following:

```
graph.traversal().V()
.or(__.out("orders").out("contains").has("idProduct", "product 10"),
__.out("orders").out("contains").has("idProduct", "product 20")).dedup().toList();
```
* Q5. PreferenceCustomer: get the customers that have ordered a specific product that is known for being popular, more than 3 times. With this query we can create offers to the customers according to the products that they buy often. 

Gremlin query can be viewed following:

```
graph.traversal().V().has("idProduct", "product 10")
.in("contains").in("orders").groupCount().unfold().where(__.select(values).is(P.gte(3))).toList();
```

* Q6. PreferenceCustomerSimilarProducts: given two specific products that are known for being popular and similar, get the customers that have ordered one of these products at least 3 times. Same that Q5, with this query we can create offers to the customers according to the type of products that they buy often.

Gremlin query can be viewed following:
```
graph.traversal().V().has("idProduct", P.within("product 10", "product 20"))
.in("contains").in("orders").groupCount().unfold().where(__.select(values).is(P.gte(3))).toList();
```

## Contest ##

Consider the metamodel of the New Yorker Contest dataset presented in [2]. In this case we are interested in indentifying the following situations of interest:

* Q1. ParticipantRate: taking into account all contests in the system, getting the number of participants that have answered at least one question in a contest in the last month.

Gremlin query can be viewed following:

```
graph.traversal().V().as("participant")
.in("askedTo").hasLabel("Question").has("date", P.inside(1467331200000L, 1472688000000L))
.select("participant").toList();
```
* Q2. ContestParticipation: taking into consideration a specific contest, obtaining all the participants that have just answered one question.

Gremlin query can be viewed following:

```
graph.traversal().V()
.where(__.in("askedTo").in("formulates").has("idContest", 508)).dedup().toList();
```
* Q3. ChoiceNotChosen: considering a specific caption, counting how many times that caption appeared in a dueling contest question and it was not eventually chosen.

Gremlin query can be viewed following:

```
graph.traversal().V()
.and(__.out("contains").has("idCaption", 61),
__.in("answers").out("chooses").has("idCaption", P.neq(61))).toList();
```
* Q4. FunniestCaption: getting the highest scored caption in a cardinal contest. The highest scored caption is considered the most voted caption tagged as 'funny'.

Gremlin query can be viewed following:

```
graph.traversal().V().as("caption")
.in("contains").out("askedTo").out("answers").has("rate", 3)
.select("caption").groupCount().unfold().order().by(values, Order.desc).select(keys).limit(1).toList();
```
* Q5. FledParticipant: obtaining all participants that answered one question only. This query might be useful when deleting participant's answers considered as irrelevants.

Gremlin query can be viewed following:

```
graph.traversal().V().has("rate", P.gt(0))
.in("answers").groupCount().unfold().where(__.select(values).is(P.eq(1))).select(keys).dedup().toList();
```
* Q6. FunniestCaptionUnbiased: same as FunniestCaption, obtaining the highest scored caption taking into account all questions generated by a random algorithm only. In this way, the result of this query is unbiased. 

Gremlin query can be viewed following:

```
graph.traversal().V().as("caption")
.in("contains")
.and(__.in("generates").has("label", P.within("RandomSampling", "RoundRobin")),
__.out("askedTo").out("answers").has("rate", 3))
.select("caption").groupCount().unfold().order().by(values, Order.desc).select(keys).limit(1).toList();
```

## Youtube ##

Consider the metamodel of the YouTube-BoundingBoxes dataset presented in [3]. In this case we are interested in indentifying the following situations of interest:

* Q1. GetAnimalVideos: obtaining all videos that contains an animal. Animal tags in this dataset are the following: "cat","dog","bird","zebra","cow","bear","horse","giraffe" and "elephant".

Gremlin query can be viewed following:

```
graph.traversal().V()
.where(__.out("contains").has("className",P.within("cat","dog","bird","zebra","cow","bear","horse","giraffe","elephant"))).toList();
```

* Q2. SegmentAbsence: getting the segments where the object is not present in any of its frames.

Gremlin query can be viewed following:

```
graph.traversal().V().hasLabel("Segment").not(__.out("contains").has("presence",1)).toList();			
```

* Q3. AnimalHumanInteraction: returning all videos that contains at least an animal and a person.

Gremlin query can be viewed following:

```
graph.traversal().V()
.and(__.out("contains").has("className","person"), 
__.out("contains").has("className",
P.within("cat","dog","bird","zebra","cow","bear","horse","giraffe","elephant"))).toList();
```

* Q4. PresenceAtBeginning: obtaining all videos with the object is present during the first 3 seconds.

Gremlin query can be viewed following:

```
graph.traversal().V().as("video")
.out("composed").and(__.out("contains")
.has("timestamp",3000).has("presence", 1),__.out("contains")
.has("timestamp", 2000).has("presence", 1),__.out("contains")
.has("timestamp", 1000).has("presence", 1)).select("video").dedup().toList();
```

* Q5. DomesticAnimalPicture: getting all frames that contains a cat or a dog.

Gremlin query can be viewed following:

```
graph.traversal().V().has("presence",1)
.or(__.in("contains").out("tracks").has("className","dog"),
__.in("contains").out("tracks").has("className","cat")).toList();
```

* Q6. CrowededDetectionVideo: returning all videos where the object is present in at least 10 segments. 

Gremlin query can be viewed following:

```
graph.traversal().V().as("video").out("composed").where(__.out("contains").has("presence",1))
.select("video").groupCount().unfold().where(__.select(values).is(P.gt(10))).select(keys).toList();
```

# Running the case studies

This section gives instructions about how to run the source code in this repository.

## Requirements/Dependencies

   * Eclipse IDE (tested with Eclipse version 2018-09 (4.9.0)).
   * Java 8
   
## Configuration and execution

In order to run the experiments, the reader has to follow some previous steps:

1. Import Java projects into a workspace.

2. Download the Source Models for each case study from [here](https://drive.google.com/open?id=1QsT5qbZLNie49hF818lQScoaJRoKHjyB) and copy them into the main folder of the project (folders _AmazonCase_, _ContentCase_ or _YoutubeCase_ depending on the case study).

3. Copy the file _yt\_bb\_detection\_train.csv_ located [here](https://drive.google.com/open?id=1BfIAW0I__1jxIx-K2imW3mRt4HiP6I7T) into the folder _YoutubeCase/src/main/resources_.

Our repository is composed by an artifact for each case study. Moreover, each artifact contains three runnable files in turn:

* _\<CaseStudy\>SubgraphApp.java_: it is used to obtain a subgraph from a graph contained in a .graphml file using the QCB algorithm.
* _\<CaseStudy\>App.java_: it is used to run a query over a graph or subgraph contained in a .graphml file.
* _\<CaseStudy\>IncApp.java_: it is used to run the incremental QCB algorithm starting from a graph stored in a .graphml file with a specific value of α and β.

Note: _\<CaseStudy\>_ must be replaced by _AmazonCase_, _ContestCase_ or _YoutubeCase_ depending on the case study.

### Obtaining a subgraph

In order to obtain a subgraph from a graph stored in a .graphml file, the reader has to follow the following steps:

1. Open file 'config.properties' located in _\<CaseStudy\>_/src/main/resources. This file contains the configuration to run the experiments. In this case, the properties to be modified are the following:

    * Change the property 'file' to indicate the Source Model to be loaded.
    
    * Change the property 'nameWeights' with an informative label. This property will be used to set the name of the .graphml file where the subgraph will be stored and the .log file name. We recommend to set this property according to the name of the source model. In this way, the file names will have the following structure: _\<QueryName\>\<nameWeights\>.graphml_ and _MyLog\<CaseStudyName\>File\<nameWeights\>.log_, respectively.
  
    * Change the property 'query' to indicate the number of query of the case study to be run. In this case, notice that Amazon case study allows values from 1 to 7 whereas Contest and Youtube cases allow values from 1 to 6. 
    
    * The rest of properties are not modified.
 
 2. Once the configuration is selected, set the Java memory heap to 10G and run the file _\<CaseStudy\>SubgraphApp.java_. 
 
 3. After a few seconds, the program will create two files in the main folder of the project: (i) a .graphml file with the resulting subgraph and (ii) a .log file with the execution time consumed to calculate the subgraph in milliseconds.
 
 ### Running a query over a graph or a subgraph

In order to run a query over a graph or a subgraph, the reader has to follow the following steps:

1. Open file 'config.properties' located in _\<CaseStudy\>_/src/main/resources. In this case, the properties to be modified are the following:

    * Change the property 'file' to indicate the .graphml file to be loaded. According to the experiment, this file can correspond to a file with a stored graph or subgraph.
    
    * Change the property 'nameWeights' with an informative label. This property will be used to set the name of the .log file. We recommend to set this property according to the name of the loaded .graphml file. In this case, the file name will have the following structure: _MyLog\<CaseStudyName\>File\<nameWeights\>.log_.
  
    * Change the property 'query' to indicate the number of query of the case study to be run. Notice that Amazon case study allows values from 1 to 7 whereas Contest and Youtube cases allow values from 1 to 6. 
    
    * The rest of properties are not modified.
 
 2. Once the configuration is selected, set the Java memory heap to 10G and  run the file _\<CaseStudy\>App.java_.
 
 3. After a few seconds, the program will return the result of six runs of the query over the .graphml file and the execution time for each run in the console. Moreover, it will create the .log file with all this information.
 
  ### Running the incremental QCB algorithm

In order to run the incremental QCB algorithm, the reader has to follow the following steps:

1. Open file 'config.properties' located in _\<CaseStudy\>_/src/main/resources. In this case, the properties to be modified are the following:

    * Change the property 'file' to indicate the Source Model to be loaded.
    
    * Change the property 'nameWeights' with an informative label. This property will be used to set the name of the .log file. We recommend to set this property according to the name of the source model file. 
  
    * Change the property 'query' to indicate the number of query of the case study to be run. Notice that Amazon case study allows values from 1 to 7 whereas Contest and Youtube cases allow values from 1 to 6. 
    
    * Change the property 'records' to select a β value.
    
    * Change the property 'recordsQuery' to select an α value.
    
    * Set the property 'incremental' to 'true' to test the _Inc_ architecture. Otherwise, to test the _NInc_ architecture, set this property to 'false'.
 
 2. Once the configuration is selected, set the Java memory heap to 10G and  run the file _\<CaseStudy\>IncApp.java_.
 
 3. After a few seconds, the program will start to show the results of each query execution in the console and the execution time of the experiment. When the program finishes, it will create a .log file with the console information. In this case, the .log file name will have the following structure: _MyLog\<CaseStudyName\>FileIncremental\<nameWeights\>-\<query\>-\<records\>-\<recordsQuery\>.log_.

 
# References

[1] Gala Barquero, Javier Troya, Antonio Vallecillo: Optimising Query Performance in Data Streaming Applications. Submitted.

[2] Data from the New Yorker Caption Contest: [https://github.com/nextml/caption-contest-data](https://github.com/nextml/caption-contest-data)

[3] Esteban Real, Jonathon Shlens, Stefano Mazzocchi, Xin Pan, Vincent Vanhoucke: YouTube-BoundingBoxes Dataset [https://research.google.com/youtube-bb/](https://research.google.com/youtube-bb/) 
