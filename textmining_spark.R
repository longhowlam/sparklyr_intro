library(dplyr)
library(sparklyr)
library(DBI)
library(stringr)

sc <- spark_connect(master = "local", version = "2.4.0")

Jaap = readRDS( "JaapText.RDs") %>% filter(!is.na(prijs), longdescription != "" ) %>%  sample_n(size = 1000)
Encoding(Jaap$longdescription) = "latin1"

Jaap = `JaapResults2017-05-28` %>% 
  filter(!is.na(prijs), longdescription != "" ) %>% 
  select(longdescription, prijs) %>% sample_n(size=100) %>% mutate(longdescription = str_trim(str_sub(longdescription,1,200)))
Jaap$longdescription=  iconv(Jaap$longdescription, "latin1", "ASCII", sub="")
jaap_tbl = copy_to(sc, Jaap)

test = data.frame(
  text = c( "Hi I heard about Spark", "I wish Java could use case classes", "Logistic regression models are neat"),
  target = c(0,0,1)
)

test_tbl = copy_to(sc, test)

testout = test_tbl %>%
  ft_tokenizer(input_col = "text", output_col = "tokens") %>%
  ft_count_vectorizer(input_col = "tokens", output_col = "counts") %>% 
  ft_idf(input_col = "counts", out) 

sdf_register(testout, "ppp")
