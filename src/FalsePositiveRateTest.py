import mmh3


def get_false_positive_rates(rdd, bloom_filters, bloom_parameters):
    """
    Given a rdd in the form (film, rating) and the bloom filters constructed return a dictionary
    of the form {rating: false positive rate}.
    Input: rdd int the form (film, rating), bloom filters, bloom parameters.
    Output: dictionary {rating: false positive rate}

    """
    false_positive_rates = rdd.map(lambda x: [round(float(x[1])), x[0]])  # map the rdd in the form (rating, film)
    false_positive_rates = false_positive_rates.flatMap(lambda x: get_false_positives(x, bloom_parameters.value,
                                                                                      bloom_filters.value))  # return a
    # list of lists where each list is in the form [rating, 1]
    # if the film is a false positive for the bloom filter related to that rating.
    false_positive_rates = false_positive_rates.reduceByKey(
        lambda a, b: a + b)  # reduce by key the number of false positives for every rating
    # get the false positive rate for every rating
    false_positive_rates = false_positive_rates.map(lambda x: get_false_positive_rate(x,
                                                                                      bloom_parameters.value)). \
        sortByKey()

    false_positive_rates.saveAsTextFile(f'./Data/Output/FalsePositiveRates')
    false_positive_rates = false_positive_rates.collect()
    false_positive_rates = {rate[0]: rate[1] for rate in false_positive_rates}
    return false_positive_rates


def get_false_positives(row, bloom_parameters, bloom_filters):
    """
    Given (rating, film) return a list of lists where each list is in the form [rating, 1]
    if the film is a false positive for the bloom filter related to that rating.
    Input: (rating, film).
    Output: list[(rating, 1)]
    """
    original_rating = row[0]
    film = row[1]
    ratings = bloom_filters.keys()
    false_positive_counter = []
    for rating in ratings:
        if original_rating != rating:
            counter = 0
            m = bloom_parameters[rating][1]
            k = bloom_parameters[rating][2]
            for i in range(k):
                hash_value = (mmh3.hash(film, i, signed=False) % m)
                if bloom_filters[rating][hash_value] == 1:
                    counter += 1
            if counter == k:
                false_positive_counter.append([rating, 1])
    return false_positive_counter


def get_false_positive_rate(row, bloom_parameters):
    """
    Given a list [rating, falsePositives] return a dictionary of type {rating: falsePositiveRate}
    Input: [rating, falsePositives].
    Output: {rating, falsePositivesRate}
    """
    rating = row[0]
    false_positives = row[1]
    values = bloom_parameters.values()
    films = 0
    for value in values:
        films += value[0]
    true_negatives = films - bloom_parameters[rating][0]
    false_positive_rate = false_positives / (false_positives + true_negatives)
    return [rating, false_positive_rate]
