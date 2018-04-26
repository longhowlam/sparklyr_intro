## build in now in sparklyr
library(sparklyr)
library(dplyr)

transactions = readRDS("transactions.RDs")

#### upload to spark ##########################################################  
trx_tbl  = copy_to(sc, transactions, overwrite = TRUE)

#### The data needs to be aggregated per id and the items need to be in a list
trx_agg = trx_tbl %>% group_by(id) %>% summarise(items = collect_list(item))

fp_model = ml_fpgrowth(trx_agg, "items", min_confidence = 0.01, min_support = 0.01)

outputdata = ml_association_rules(fp_model) %>% 
  sdf_separate_column("antecedent", c("RHS1", "RHS2")) %>%
  sdf_separate_column("consequent") %>% 
  collect()
  

ml_freq_itemsets(fp_model) 
