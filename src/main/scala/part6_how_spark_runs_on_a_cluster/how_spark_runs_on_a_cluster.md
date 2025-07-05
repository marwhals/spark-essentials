# How Spark Runs on a Cluster
---

- Anatomy of a Spark Job
  - stages
  - tasks
  - shuffles
  - the DAG (directed acyclic graph)
- Execution Terminology
  - A job has stages, stage has tasks
  - **Stage** = a set of computations between shuffles
  - **task** = a unit of computation, per partition
  - **the DAG** = a graph of RDD dependencies
- Key points
  - Spark performs optimisations and generates steps. Can split a job between multiple jobs or even eliminate jobs altogether if an operations can be optimised.
  - Physical Plans - Describes all the steps Spark will execute before running any actual code on the cluster

---

[//]: # (TODO - use spark cluster in project)