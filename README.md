# Formation Hadoop TP TF-IDF

## First MR job

### Mapper:
1. Remove non letter character
2. Remove less than three letter words
3. Transform all letter to lower case
4. Remove useless word (based on a stopword list)

Output:
* key -> (word, document identifier)
* value -> ONE constant

### Reducer:

