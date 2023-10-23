# map-reduce
This project is cumulitive result of attemps to analyse BIG DATA. 
Free data set is provided by MovieLens(https://grouplens.org/datasets/movielens/). In repository you can find this data in zip archive - m1-1m.zip. Occupation.dat is list of users occupations (key-value format). Users.dat give information about about every user, who took part in the survey. All ratings are contained in the file "ratings.dat". The main aim was to find which "profession" love watching films nore than other. More information in local README file.
Apache Hadoop product as main insrument was choosen and map-reduce functions was written using Java(Maven build). You can use package - map-reduce-0.0.1-SNAPSHOT.jar or you can transform map-reduce function and build package by yourself - source and pom lie in map-reduce folder.
I used multy-cluster hadoop architecture with 3 data-nodes (Virtual Box gave me such opportunity). 
Results in part-r-00000. "Retired" group has maximum rating - 3.78 and unemployed minimum  - 3,41.
