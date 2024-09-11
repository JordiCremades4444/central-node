Rules for this repo

1 - Enforce minimalism

2 - Do not create complex scripts

3 - Mantain folder structure accross all Glovo notebooks, the possible folders are:

outputs: To store outputs of the analysis that will not be consumed by any other script
to_load: Objects to be consumed by scripts. Originally from other scripts or alien to the repo
queries: All queries used by each notebook. If there is only one notebook do not subdivide in folders. Otherwise do.
query_logs: To store last runs of the queries. 
