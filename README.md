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
* Q6. FunniestCaptionUnbiased: same as FunniestCaption, obtaining the highest scored caption taking into account all answers generated by a random algorithm only. In this way, the result of this query is unbiased. 

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

In order to run the case study, the reader has to follow the following steps:

1. Import Java projects into a workspace.

...
 
 # Experiment Results

 
# References

[1] Gala Barquero, Javier Troya, Antonio Vallecillo: Optimising Query Performance in Data Streaming Applications. Submitted.

[2] Data from the New Yorker Caption Contest: [https://github.com/nextml/caption-contest-data](https://github.com/nextml/caption-contest-data)

[3] Esteban Real, Jonathon Shlens, Stefano Mazzocchi, Xin Pan, Vincent Vanhoucke: YouTube-BoundingBoxes Dataset [https://research.google.com/youtube-bb/](https://research.google.com/youtube-bb/) 
