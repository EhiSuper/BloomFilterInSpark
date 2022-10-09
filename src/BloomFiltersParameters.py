from math import log


def get_bloom_filters_parameters(rdd, false_positive_ratio):
    """
    Given a rdd of films and ratings return a dictionary of parameters n, m ,k for every rating.
    Input: rdd of filmId, rating.
    Output: Dictionary of {rating: [n, m, k]} sorted for rating.
    """
    rdd = rdd.map(lambda x: [round(float(x[1])), x[0]])  # map the rdd in the form (rating, film)
    rdd = rdd.groupByKey()
    rdd = rdd.map(lambda x: get_parameters(x, false_positive_ratio))  # map parameters to every rating
    rdd = rdd.sortByKey()  # sort ratings
    rdd.saveAsTextFile(f"./Data/Output/Parameters")
    bloom_parameters = rdd.collect()
    bloom_parameters = {parameter[0]: parameter[1] for parameter in bloom_parameters}  # transform the list of lists
    # in a dictionary
    return bloom_parameters


def get_parameters(rating, false_positive_ratio):
    """
    Return parameters n,m and k for the Bloom filter construction.
    Input: (rating, list[filmId])
    Output: (rating, [n, m, k])
    """
    n = len(rating[1])
    m = round(-((n * log(false_positive_ratio)) / (log(2)) ** 2))
    k = round((m / n) * log(2))
    return rating[0], [n, m, k]
