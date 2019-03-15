#### SENTIMENT analysis ##########################################################################
##
## Example text mining functionality on Spark


# start up code ----------------------------------------------------------------------------------
library(dplyr)
library(sparklyr)
library(DBI)
library(stringr)
library(readr)
library(ggplot2)

sc <- spark_connect(master = "local", version = "2.4.0")

# Import the review data -------------------------------------------------------------------------
# non ascii characters need to be removed....

imdb <- read_csv("imdb_master.csv")  %>%  select(type, review, label)
imdb$review = iconv(imdb$review, "latin1", "ASCII", sub="")

# lets focs on negative and positive reviews only
table(imdb$label)

imdb = imdb %>% 
  filter(
    label %in% c("neg","pos")
  )

# the label needs to be a 0 /1 datatype
imdb = imdb %>% 
  mutate(
    label = ifelse(label == "pos", 1, 0)
  )

# split into train and test data
imdb_train = imdb %>% filter(type == "train") %>%  select(review, label)
imdb_test = imdb %>% filter(type == "test")  %>%  select(review, label)

# copy data to spark -----------------------------------------------------------------------------
imdb_tbl = copy_to(sc, imdb_train, overwrite = TRUE)
imdb_test_tbl = copy_to(sc, imdb_test, overwrite = TRUE)


####################################################################################################
######  TRAIN logistic model, target is in column 'label'  #########################################


# The following feature transformers are applied to the reviews
# Full documentation on Spark feature transformers: https://spark.apache.org/docs/latest/ml-guide.html

# 1. tokenize : breaking up sentences in tokens (usually words)
# 2. count the tokens, how much does each each token occur
# 3. Create an inverse document frequency (scaling down tokens that occur (too) often)


# things can be run as individual steps:

tokens = imdb_tbl %>%
  ft_tokenizer(
    input_col = "review",
    output_col = "tokens"
  )

counts = tokens %>%
  ft_count_vectorizer(
    input_col = "tokens", 
    output_col = "counts"
  ) 

idf = counts %>% 
  ft_idf(
    input_col = "counts", 
    output_col = "features"
  )

model = idf %>% 
  ml_logistic_regression()


##### Machine learning pipeline ###################################

df = imdb_tbl %>% select(review, label)

review_model_pipeline <- ml_pipeline(sc) %>%
  ft_dplyr_transformer(
    tbl = df
  ) %>% 
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
  ml_logistic_regression(
    
  )


review_model_pipeline


### train the pipeline
fitted_review_model_pipeline <- ml_fit(
  review_model_pipeline,
  imdb_tbl
)


##### test model on test set --------------------------------------------------------------------------------------
predictions <- ml_transform(
  fitted_review_model_pipeline,
  imdb_test_tbl
)

ml_binary_classification_evaluator(
  predictions
)
## AUC  0.9052197


#### visual inspection, observed positive ratios per score band ----------------------------------------------------------------

testpred = predictions %>% sdf_separate_column("probability", into = c("P", "Q")) %>% select(P,Q, label, review) %>%  collect()

testpred = testpred %>% 
  mutate(
    predclass = cut(Q, breaks = quantile(Q, (0:10)/10))
  )

p = testpred %>% 
  group_by(
    predclass
  ) %>% 
  summarise(
    n=n(),
    observed  = mean(label)
  ) %>% 
  ggplot(
    aes(predclass, weight = observed)
  ) +
  geom_bar()

p

