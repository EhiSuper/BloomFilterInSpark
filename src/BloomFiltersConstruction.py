import mmh3

def getBloomFilters(rdd, bloom_parameters):
    """
    Given an rdd in the form (film, rating) return a dictionary with one bloom filter for every rating.
    Input: rdd in the form (film, rating).
    Output: {rating: bloom_filter}
    """
    rdd = rdd.map(lambda x: [round(float(x[1])), x[0]]) # map the rdd in the form (rating, film)
    indexes = rdd.map(lambda x: getIndexes(x, bloom_parameters)) # map the rdd in the form (rating, list[indexes]) 
    joined_indexes = indexes.reduceByKey(lambda x, y: concatenateIndexes(x, y)) # join the list related to every rating
    bloom_filters = joined_indexes.map(lambda x: createBloomFiltersFromIndexes(x, bloom_parameters)).sortByKey() # for every rating calculate the relative bloom filter based on the indexes
    bloom_filters.saveAsTextFile("Data/BloomFilters")
    bloom_filters = bloom_filters.collect()
    bloom_filters = {list[0]:list[1] for list in bloom_filters} # create the dictionary from the list of lists
    return bloom_filters

def getIndexes(row, bloom_parameters):
    """
    Return the indexes of the bloom filter of a specific film related to its rating.
    Input: (rating, filmId)
    Output: (rating, {indexes})
    """
    indexes = []
    rating = row[0]
    film = row[1]
    m = bloom_parameters[rating][1]
    k = bloom_parameters[rating][2]
    for i in range(k):
        indexes.append((mmh3.hash(film, i, signed=False) % m))
    return [rating, indexes]

def concatenateIndexes(x, y):
    """
    Given 2 lists x, y return a list that is the union of the two.
    Input: list , list.
    Ountput: list.
    """
    x.extend(y)
    return x

def createBloomFiltersFromIndexes(row, bloom_parameters):
    """
    Given a set of ratings and indexes return the relative bloom filters
    Input: (rating, list[indexes])
    Output: [rating, bloom_filters]
    """
    rating = row[0]
    indexes = row[1]
    m = bloom_parameters[rating][1]

    bloom_filters = [0] * m # creation of the array of dimension m
    for index in indexes:
        bloom_filters[index] = 1
    return [rating, bloom_filters]


    