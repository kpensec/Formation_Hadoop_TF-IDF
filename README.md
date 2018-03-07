# Formation Hadoop TP TF-IDF

## First MR job

### Mapper:
1. Remove non letter character
2. Remove less than three letter words
3. Transform all letter to lower case
4. Remove useless words (based on a stopwords list)

#### Output:
* key -> (word, document identifier)
* value -> ONE constant

### Reducer:
Word count 
