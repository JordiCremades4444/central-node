-- sequemce
-- returns an array that contains integers that go from 0 to 10, with step of 1 size
    sequence(0,10,1)
-- it can be used to create sequences of double with the following trick
    select x/10.0 from unnest((sequence(0, 10, 1))) as t(x)


-- beta_cdf
-- returns the beta cdf value from the beta distribution build with A and B parameters
-- we use the following trick to plot different values
    with x_axis as (select
            x/10.0 as x_value
        from unnest((sequence(0, 10, 1))) as t(x)
    )

    select 
        x_value,
        beta_cdf(1.0, 1.0, x_value) AS cdf_values
    from x_axis
-- we use the inverse_beta to get the inverse given an alpha and beta parameters
    inverse_beta(1.0, 1.0, 0.5)

--normal_cdf
--returns the normal cdf value from the normal distribution built with mean and std parameters
    normal_cdf(1.0,1.0,0.5)
    inverse_normal(1.0,1.0,0.5)

-- bool_and
-- return true if every input value is true
    select bool_and(order_is_pna) from orders

-- bool_or
-- return true if one of the values is true
    select bool_and(order_is_pna) from orders

-- cardinality
-- returns the size of an array. Also applicable to maps.
    cardinality(array_agg(order_id))

-- cbrt
-- cube root of a scalar
    cbrt(27) = 3

-- ceil or ceiling/ floor
-- round up to the nearest integer/ round down to the nearest integer
    ceil(3) = 3, ceil(3.1) = 4
    floor(3) = 3, floor(3.1) = 3

-- combinations
-- returns de n possible subsets of size xxx. The result pairs are provided in an array.
    combinations(a, 2)

-- concat_ws
-- concatenate using separator in the first position
    concat_ws('_',order_id,store_id)

-- contains
-- returns true if an array contains a certain element
    contains(a,'wrong or missing')

-- contains_sequence
-- returns true if an array contains a certain sequence
    contains_sequence(a, array['element1','element2'])

-- corr
-- pearson correlation between two columns.  
    corr(gmv, number_of_orders)

-- cosine_similarity
-- provides the cosine similarity between 2 vectors. It ranges from 0 to 1
    SELECT cosine_similarity(
        map(array['x', 'y', 'z'], array[1, 0, 0]),
        map(array['x', 'y', 'z'], array[0, 1, 0])
    ) AS similarity_score

-- count_if
-- countif condition is met
    count_if(order_is_pna)

-- covar_pop
-- covariance of 2 populations
    covar_pop(gmv, number_of_orders)

-- covar_samp
-- covariance of 2 samples. Use it if you are not working with the whole population
    covar_samp(gmv, number_of_orders)

-- date_add
-- add or subtract times
    date_add('day', 1, current_date)

-- date_diff
-- time difference between 2 times
    date_diff('day', init_date, final_date)
    date_diff('second', init_timestamp, final_timestamp)

-- date_format
-- format a timestamp into a given string format
    date_format(timestamp '2023-01-01 05:00:00', '%m-%d-%Y %H')

-- date_parse
-- format a string to a timestamp
    date_parse('2023/01/01/05', '%Y/%m/%d/%H')

-- date_trunc
-- truncate the unit from a date or timestamp
    select date_trunc('month' , date('2023-01-06')) = 2023-01-01
    select date_trunc('day' , timestamp '2023-01-06 05:00:00') = 2023-01-06 00:00:00

-- day
-- returns day of the month
    select day(date('2023-01-06')) = 6
    select day(timestamp '2023-01-06 05:00:00') = 6

-- day_of month, day_of_year, day_of_week, hour, last_day_of_month
-- similar to above for timestamps

-- dow, doy, hour,last_day_of_month 
-- similar to above for dates

-- degree
-- converts from radiants to degree
    degree(pi()) = 180

-- element_at
-- returns the element at nth position for an array. Null if the position does not exist in the array.
    element_at(a,1)
-- returs the element at the key position for a map. Null if the key does not exist
    with mapping as (select map(
        ARRAY['barcelona', 'madrid', 'valencia'],
        ARRAY['90', '80', '10']
    ) AS sample_map)

    select element_at(sample_map,'barcelona') from mapping

-- filter
-- returns an array with the elements of the array that comply with the condition
    filter(array_agg(order_is_pna), x -> x = true)

-- flatten
-- returns a flattened version of nested arrays. This happens when inside an array you have more arrays.
    with mapping as (select map(
        ARRAY['barcelona', 'madrid', 'valencia'],
        ARRAY[array['bcn','barna'], array['madri'], array['val','vale']]
    ) AS sample_map)

    select 
        flatten(map_values(sample_map))
    from mapping

-- great_circle_distance
-- measures the distance in km between two positions
    great_circle_distance(s1.store_address_lat,s1.store_address_lon,s2.store_address_lat,s2.store_address_lon)

-- greatest
-- from a set of numbers returns the greatest value
    greatest(order_id)

-- histogram
-- returns a map with the histogram frequency
    histogram(order_city_code)

-- how to unnest a map
    WITH orders AS (
        SELECT *
        FROM delta.central__pna_orders__odp.pna_orders_info_v2
        LIMIT 100
    )

    -- This creates a map with cities -> frequency
    , city_histogram as (select 
        histogram(order_city_code) as h
    from orders)

    -- This unnests the map
    select 
        city
        ,value
    from city_histogram
    cross join unnest(map_entries(h)) as t(city,value)

-- index
-- returns the position of a substring. First occurrence
    index('cocacola','a') = 4

-- map_agg
-- aggregates the keys and values for a map
    WITH orders AS (
        SELECT *
        FROM delta.central__pna_orders__odp.pna_orders_info_v2
        LIMIT 100
    )

    select 
        order_country_code,
        map_agg(order_city_code, order_id)
    from orders
    group by order_country_code

-- map_entries
-- returns an array of all the entires on a given map. Returning all the keys and values
map_entries(m)

-- map_filter
