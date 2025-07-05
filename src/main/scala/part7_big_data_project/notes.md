# A Big Data Problem

---

# Outline of a big data project

- Data gathering
- Investigative analysis
- Package application
- Deploy and run on Amazon EMR (Elastic Map Reduce)

---

# The Scenario

- The NYC taxi want to improve their business. They think it is a good idea to use Spark.
- Data: all the taxi rides between 2009 and 2016.
- Requirements
    - Gather data insights
    - Make proposals for business improvements
    - Suggest one potential approach
    - Evaluate the impact over the existing data

### The Data

- Data: 1.4 billion taxi rides between 2009 and 2016
    - 35GB snappy Parquet -400GB uncompressed CSV
    - Breaking the Big Data Barrier (not really feasible to put this on a single machine)
    - This data can be found online [Link to Data](https://.....)
- A big DataFrame containing all taxi rides
    - `tpep_pickup_datetime` = pickup timestamp
    - `tpep_dropoff_datetime` = dropoff timestamp
    - `passenger_count`
    - `trip_distance` = length of the trip in miles
    - RatecodeID = 1 (Standard), 2 (JFK), 3 (Newark), 4 (Nassau / Westchester) or 5 (negotiated)
    - PULocationID = pickup location zone ID
    - DOLocationID = dropoff location zone ID
    - `payment_type` = 1 (credit card), 2 (cash), 3 (no charge), 4 (dispute), 5 (unknown), 6 (voided)
    - `total_amount`
    - ...8 other columns in the dataframe which is not required for the analysis
    - Also have a smaller DataFrame with the taxi zone descriptions

---

### The Questions

1) Which zones have the most pickups/dropoffs overall?
2) What are the peak hours for taxi?
3) How are the trips distributed by length? Why are people taking the cab?
4) What are the peak hours for long/short trips?
5) What are the top 3 pickup/dropoff zones for long/short trips?
6) How are people paying for the ride, on long/short trips?
7) How is the payment type evolving with time?
8) Can we explore a ride-sharing opportunity by grouping close short trips?