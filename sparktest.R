library(sparklyr)
library(dplyr)
library(DBI)
library(ggplot2)

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


cars_tbl %>% count()

## lazy evaluation...
cars_tbl2 = cars_tbl %>% 
  mutate(
    newcol1 = 100*vs,
    newcol2 = wt + cyl
  )

## split up longer manipulation 
cars_tbl3 = cars_tbl2 %>% 
  group_by(
    cyl
  ) %>% 
  summarise(
    meancol = mean(newcol2)
  )

## collect to R:  BE CARE FULL:  DO NOT FLOOD YOUR R SESSION!!!
cars3 = cars_tbl3 %>% collect()

## now you can do ggplots or whatever you want in R.....
ggplot(cars3, aes(cyl)) + geom_bar(aes(weight = meancol))


cars3_tbl = cars_tbl %>% 
  left_join(
    cars2_tbl,
    by = c("car" = "car")
  ) %>% 
  filter(
    TEST > 0.1
  )

####  machine learning #####################################################













################## close the connection ##################################


spark_disconnect(sc)








