# How DataFrames Work
---

## Data Frames

- Distributed spreadsheets with rows and columns
- Schema = list describing the column names and types
  - types known to Spark, not at compile time. (*Typesafe way exists*)
  - arbitary number of columns
  - all rows have the same structure
- Need to be distributed
  - Data is too big for a single computer
  - Too long to process the entire data on a single CPU
- Partitioning
  - Splits the data into files, distributed between notes in the cluster
  - Impacts the processing 
- Immutable
  - Can't be changed once created
  - create other DFs via transformations
- Transformations
  - narrow = one input partition contributed to at most one output partition (e,g map)
  - wide = input partitions (one or more) create many output partitions (e.g sort)
- Shuffle = data exchange between cluster nodes
  - Occurs in wide transformations
  - *has a big impact on performance*

### Computing DataFrames
- Lazy Evaluation
  - Spark waits until the last moment to execute the DF transformations
- Planning
  - Spark compiles the DF transformations into a graph before running any code
  - Logical plan = DF dependency graph + narrow/wide transformation sequence
  - Physical plan = optimised sequence of steps for nodes in the cluster
  - optimisations
- Transformations vs Actions
  - transformations describe how new DFs are obtained
  - Actions actually start executing the Spark code