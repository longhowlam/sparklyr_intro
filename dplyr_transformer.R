library(sparklyr)
library(dplyr)
library(DBI)

sc = spark_connect(master="local")

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

