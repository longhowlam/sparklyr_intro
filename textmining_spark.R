library(dplyr)
library(sparklyr)
library(DBI)
library(stringr)

#### sentiment analysis ################################################################

sc <- spark_connect(master = "local", version = "2.4.0")

#### Import the data ####################################################################
## non ascii characters need to be removed....

imdb <- read_csv("imdb_master.csv")  %>%  select(type, review, label)
imdb$review = iconv(imdb$review, "latin1", "ASCII", sub="")

imdb_train = imdb %>% filter(type == "train") %>%  select(review, label)
imdb_test = imdb %>% filter(type == "test")  %>%  select(review, label)

imdb_tbl = copy_to(sc, imdb_train, overwrite = TRUE)
imdb_test_tbl = copy_to(sc, imdb_test, overwrite = TRUE)

######  Train on logistic model, target is in column 'label'  #####################

logregmodel = imdb_tbl %>%
  ft_tokenizer(
    input_col = "review",
    output_col = "tokens"
  ) %>%
  ft_count_vectorizer(
    input_col = "tokens", 
    output_col = "counts"
  ) %>% 
  ft_idf(
    input_col = "counts", 
    output_col = "features"
  ) %>% 
  ml_logistic_regression(reg_param = 0.5)

summary(testout)
