#############################################################################
## in sparklyr heb je nu ook directe aanroep naar fp growth in mllib

## wat data 
transactions = readRDS("transactions.RDs")
trx_tbl  = copy_to(sc, transactions, overwrite = TRUE)

#### The data needs to be aggregated per id and the items need to be in a list
trx_agg = trx_tbl %>% group_by(id) %>% summarise(items = collect_list(item))
trx_agg %>% count()


modelout = sparklyr::ml_fpgrowth(trx_agg, min_conf = 0.01, min_support = 0.01)
nLHS = 2
handle_rules = ml_association_rules(modelout) %>% 
  sdf_register("rules_out")

## regels staan in lists verpakt die je moet uitpakken

src_tbls(sc)
exprs1 <- lapply(
  0:(nLHS - 1), 
  function(i) paste("CAST(antecedent[", i, "] AS string) AS LHSitem", i, sep="")
)
exprs2 <- lapply(
  0:(nRHS - 1), 
  function(i) paste("CAST(consequent[", i, "] AS string) AS RHSitem", i, sep="")
)

splittedLHS = handle_rules %>%  spark_dataframe() %>%
  invoke("selectExpr", exprs1) %>%
  sdf_register( "tmp1")

############################################
# transform examines the input items against all the association rules and summarize the
# consequents as predictiontransform

transoutput = modelout$.jobj %>% invoke("transform", spark_dataframe(trx_agg))
transformoutput = sdf_register(transoutput,"transoutput")

