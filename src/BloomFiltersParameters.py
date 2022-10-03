from math import log

def getBloomFiltersParameters(rdd, false_positive_ratio):
    """
    Given an rdd of films and ratings return a dictionary of parameters n, m ,k for every rating.
    Input: rdd of filmId, rating.
    Output: Dictionary of {rating: [n, m, k]} sorted for rating.
    """
    rdd = rdd.map(lambda x: [round(float(x[1])), x[0]]) # map the rdd in the form (rating, film)
    rdd = rdd.groupByKey()
    rdd = rdd.map(lambda x: getParameters(x, false_positive_ratio)) # map parameters to every rating
    rdd = rdd.sortByKey() # sort ratings
    rdd.saveAsTextFile(f"./Data/Output/Parameters") 
    bloom_parameters = rdd.collect()
    bloom_parameters = {list[0]:list[1] for list in bloom_parameters} # transform the list of lists in a dictionary
    return bloom_parameters

def getParameters(rating, false_positive_ratio):
    """
    Return parameters n,m and k for the Bloom filter construction.
    Input: (rating, list[filmId])
    Output: (rating, [n, m, k])
    """
    n = len(rating[1])
    m = round(-((n*log(false_positive_ratio))/(log(2))**2))
    k = round((m/n)*log(2))
    return rating[0], [n, m, k]