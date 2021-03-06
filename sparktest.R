library(sparklyr)
library(dplyr)
library(DBI)
library(ggplot2)
library(nycflights13)

spark_available_versions()

spark_install(version = "3.0.0")

sc <- spark_connect(master = "local", version = "3.0.0")

cars = readRDS("cars.RDs")
cars2 = readRDS("cars2.RDs")


### once connected you have in RStudio the connections GUI to see spark tables
## and a link to the SPARK UI



### normally you would not put big data sets from R to Spark....
### they would come from HIVE for example or parquet, something
### that is scalable to import in paralel into spark mememory

cars_tbl = copy_to(sc,cars)
cars2_tbl = copy_to(sc,cars2)

#### dplyr statements #########################################################
## to manipulate spark data 

cars_tbl %>% count()

## lazy evaluation...
cars_tbl2 = cars_tbl %>% 
  mutate(
    newcol1 = 100*vs,
    newcol2 = wt + cyl
  )

cars_tbl2 %>%  count()

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

### joins with dplyr in spark
cars3_tbl = cars_tbl %>% 
  left_join(
    cars2_tbl,
    by = c("car" = "car")
  ) %>% 
  filter(
    TEST > 0.1
  )


#######  dplyr transformers ###################################################

iris_tbl = copy_to(sc, iris, overwrite = T)

dplyr_stmts = iris_tbl %>%
  group_by(Species) %>% 
  summarise(
    N = n()
  )

SpeciesCounter = ft_dplyr_transformer(sc, dplyr_stmts )

## just any data frame in spark with a Species columns
test = tibble( Species = c("setosa", "setosa", "virginica"))
test_tbl = copy_to(sc, test)

SpeciesCounter %>% 
  ml_transform(test_tbl)


#***************************************************************************
####  machine learning #####################################################

### vergelijkbare formula syntax als in R
mpg_lmmodel = cars_tbl %>% 
  ml_linear_regression( mpg ~ disp + cyl + hp)

summary(mpg_lmmodel)

mpg_lmmodel = cars_tbl %>% 
  ml_linear_regression( mpg ~ . - car)

mpg_rfmodel = cars_tbl %>% 
  ml_random_forest( mpg ~ . - car)


partitions = cars_tbl %>%
  filter(
    hp >= 100
  ) %>%
  sdf_mutate(
    cyl8 = ft_bucketizer(cyl, c(0,8,12))
  ) %>%
  sdf_partition(
    training = 0.5, test = 0.5, seed = 888
  )

# fit a linear mdoel to the training dataset
fit = partitions$training %>%
  ml_linear_regression(mpg ~ wt + cyl8)

# predict mpg on  on test set
pred = sdf_predict(
  partitions$test,  fit
)


####### Machine learning  pipelines ##############################

#### R piplines ####
## in R you can use functional sequences that can be used as pipelines...
r_pipeline =  . %>% 
  mutate(
    cyl = paste0("c", cyl)
  ) %>% 
  lm(
    am ~ cyl + mpg, data = .
  )
r_pipeline

# apply r pipeline on R data 
cars %>% 
  r_pipeline() %>% 
  summary()

#### SPARK ML PIPELIENS ####
## Now you can use Spark MLlib ML pipelines on Spark data within RStudio
spark_flights = sdf_copy_to(sc, flights)

## 336776 records
spark_flights %>% count()


flight_dplyr_stmts = spark_flights %>%
  filter(!is.na(dep_delay)) %>%
  mutate(
    month = paste0("m", month),
    day = paste0("d", day)
  ) %>%
  select(
    dep_delay, sched_dep_time, month, day, distance
  ) 


flights_pipeline = ml_pipeline(sc) %>%
  ft_dplyr_transformer(
    tbl = flight_dplyr_stmts
  ) %>%
  ft_binarizer(
    input_col = "dep_delay",
    output_col = "delayed",
    threshold = 15
  ) %>%
  ft_bucketizer(
    input_col = "sched_dep_time",
    output_col = "hours",
    splits = c(400, 800, 1200, 1600, 2000, 2400)
  )  %>%
  ft_r_formula(
    delayed ~ month + day + hours + distance
  ) %>% 
  ml_logistic_regression()

## let's see what's in the pipeline
flights_pipeline
class(flights_pipeline)


### use train data to fit the ml pipeline

partitioned_flights = sdf_partition(
  spark_flights,
  training = 0.8,
  testing = 0.2
)

fitted_pipeline = ml_fit(
  flights_pipeline,
  partitioned_flights$training
)

## now contains fitted coefficients
fitted_pipeline


## pipeline consists of multiple things
#fifth stage is the logistic regression from which we can extract parameters
fitted_pipeline$stages[[5]]
fitted_pipeline$stages[[5]]$coefficients

#### saving spark ml pipeline en fitted pipeline ####

ml_save(
  flights_pipeline,
  "flights_pipeline",
  overwrite = TRUE
)



### use fitted pipeline to apply on test set

reloaded_model <- ml_load(sc, "flights_model")

predictions <- ml_transform(
  reloaded_model,
  partitioned_flights$testing
)

## get evaluation metric of the predictions
ml_binary_classification_evaluator(predictions)


## Spark puts predictions into a one list column, in R you
## can't do anything with it, it needs to be separated

INR = predictions %>%
  sdf_separate_column(
    "probability",
    c("P0", "P1")
  ) %>% 
  collect()


#### long runnning spark jobs are now visible in jobs monitor in RStudio
sdf_len(sc, 10) %>% spark_apply(~ Sys.sleep(60 * 10))


################## close the connection ##################################


spark_disconnect(sc)








