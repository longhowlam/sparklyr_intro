library(sparklyr)
library(dplyr)
library(DBI)

sc <- spark_connect(master = "local")

cars = readRDS("cars.RDs")
cars2 = readRDS("cars2.RDs")


### once connected you have in RStudio the connections GUI to see spark tables
## and a link to the SPARK UI


### normally you would not put big data sets from R to Spark....
### they would come from HIVE for example or parquet, something
### that is scalable to import in paralel into spark mememory

cars_tbl = copy_to(sc,cars)
cars2_tbl = copy_to(sc,cars2)




#### dplyr statements to manipulate spark data #############################



####  machine learning #####################################################


spark_disconnect(sc)








