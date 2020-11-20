# Limpando dados ----------------------------------------------------------

rm(list = ls())
graphics.off()

# Pacotes -----------------------------------------------------------------

library(tidyverse)
library(BatchGetSymbols)
library(GetDFPData2)
library(quantmod)
library(lubridate)
library(scales)
library(tidyquant)
library(cluster)
library(factoextra)
library(ggdendro)
library(GGally)

# Functions ---------------------------------------------------------------

source("fcts/correlation_matrix.R")
source("fcts/calculo_indices.R", encoding = "UTF-8")

# Empresas ----------------------------------------------------------------

international_ticker.ce <- c('ARCE')

tickers.ce <- c(
  "PGMN3.SA",
  "BNBR3.SA",
  # 'COCE3.SA',
  'COCE5.SA',
  # "COCE6.SA",
  'GRND3.SA',
  'MDIA3.SA',
  'HAPV3.SA'
)

# Parâmetros --------------------------------------------------------------

df_info <- get_info_companies("data/empresas_bolsa")
df_info.ce <- df_info %>% 
  filter(UF == "CE")

cdg_empresas_cvm <- c(
  1228, # Banco do Nordeste
  14869, # Coelce
  19615, # Grendene
  20338, # M. Dias Branco
  24392
)

inicial.date <- ymd('2015-01-01')
final.date <- today()
tresh_bad_data <- 0.01
cache_folder <- 'data/cotacao'
retorno <- "log"

# Importando dados --------------------------------------------------------

# Ibovespa

ibovespa <- BatchGetSymbols::BatchGetSymbols(
  "^BVSP",
  first.date = "2010-01-01",
  last.date = today(),
  thresh.bad.data = 0.01,
  cache.folder = cache_folder,
  type.return = retorno
)

# Empresas nacionais

empresas.ce <- BatchGetSymbols(
  tickers = tickers.ce,
  first.date = inicial.date,
  last.date = final.date,
  thresh.bad.data = tresh_bad_data,
  cache.folder = cache_folder,
  bench.ticker = '^BVSP',
  type.return = retorno
)

# Empresas internacionais

empresas.ce_internacional <- BatchGetSymbols(
  tickers = international_ticker.ce,
  first.date = inicial.date,
  last.date = final.date,
  thresh.bad.data = tresh_bad_data,
  cache.folder = cache_folder,
  bench.ticker = '^BVSP',
  type.return = retorno
)

# Transformando em data frames

empresas.ce.df <- empresas.ce$df.tickers %>% 
  as_tibble() %>% 
  drop_na() %>% bind_rows(
    empresas.ce_internacional$df.tickers %>% 
      as_tibble() %>% 
      drop_na()
  )


wide.empresas.ce <- reshape.wide(empresas.ce$df.tickers)


  # empresas_inter.ce.df <- empresas.ce_internacional$df.tickers %>% 
#   as_tibble() %>% 
#   drop_na()

# Importando informações individuais

getSymbols(tickers.ce, from = inicial.date)
getSymbols(international_ticker.ce, from = inicial.date)
getSymbols("^BVSP", src = "yahoo", from = inicial.date) # Ibovespa

# Imformações das empresas brasileiras ------------------------------------

# Empresas Cearenses

empresas_selecionadas <- df_info.ce %>%
  select(
    UF,
    CIDADE,
    DENOM_COMERC,
    SETOR_ATIV,
    CNPJ,
    DT_CONST,
    DT_REG,
    TP_MERC,
    SIT_REG
  ) %>%
  filter(SIT_REG == "ATIVO") %>% 
  mutate(
    # DT_CONST = parse_date(DT_CONST, "%d/%m/%Y"),
    DT_REG = parse_date(DT_REG, "%d/%m/%Y"),
    CNPJ = as_factor(CNPJ),
    TP_MERC = ifelse(
      DENOM_COMERC == "EMPREENDIMENTOS PAGUE MENOS SA",
      "Bolsa",
      TP_MERC
      )
    ) %>%
  mutate_all(~ str_to_sentence(.))
  

empresas_selecionadas_cvm <- empresas_selecionadas %>% 
  mutate(
    TP_MERC = factor(TP_MERC, levels = c("Bolsa", "Balcão não organizado")),
    DENOM_COMERC = str_to_upper(DENOM_COMERC)
  ) %>% 
  arrange(TP_MERC) %>% 
  select(-DT_REG, -SIT_REG, -CNPJ) %>% 
  rename(
    Cidade = CIDADE,
    "Denominação Comercial" = DENOM_COMERC,
    "Setor de Atividade" = SETOR_ATIV,
    "Data da Constituição" = DT_CONST,
    "Tipo de Mercado" = TP_MERC
  ) %>% 
  mutate(
    `Tipo de Mercado` = as.character(`Tipo de Mercado`),
    "Tipo de Mercado" = ifelse(is.na(`Tipo de Mercado`), "", `Tipo de Mercado`)
  ) 

empresas_selecionadas_cvm$UF[empresas_selecionadas_cvm$UF == "Ce"] <- "Ceará"


# Tabela 1 - Empresas cearenses -------------------------------------------

tab1 <- empresas.ce$df.control %>% 
  bind_rows(empresas.ce_internacional$df.control) %>% 
  select(-src, -download.status, -perc.benchmark.dates, -threshold.decision) %>% 
  arrange(ticker) %>% 
  left_join(
    empresas.ce.df %>% 
      select(ticker, volume, price.adjusted, ret.adjusted.prices) %>% 
      group_by(ticker) %>% 
      summarise(
        volume_mean = mean(volume),
        price.adjusted_mean = mean(price.adjusted),
        ret.adjusted.prices_mean = mean(ret.adjusted.prices)*100,
      ) %>% 
      ungroup() %>%
      arrange(ticker),
    by = "ticker"
  )

# Tabela 2 - Retorno anual ------------------------------------------------

tab2 <- yearlyReturn(PGMN3.SA) %>%
  fortify() %>% 
  as_tibble() %>% 
  mutate(
    ticker = "PGMN3"
  ) %>% bind_rows(
    
yearlyReturn(BNBR3.SA) %>%
  fortify() %>% 
  as_tibble() %>% 
  mutate(
    ticker = "BNBR3"
  ),

yearlyReturn(COCE5.SA) %>% 
  fortify() %>%
  as_tibble() %>% 
  mutate(
    ticker = "COCE5"
  ),

yearlyReturn(GRND3.SA) %>% 
  fortify() %>% 
  as_tibble() %>% 
  mutate(
    ticker = "GRND3"
  ),

yearlyReturn(MDIA3.SA) %>% 
  fortify() %>%
  as_tibble() %>% 
  mutate(
    ticker = "MDIA3"
  ),

yearlyReturn(HAPV3.SA) %>%
  fortify() %>%
  as_tibble() %>% 
  mutate(
    ticker = "HAPV3"
  ),
yearlyReturn(ARCE) %>%
  fortify() %>%
  as_tibble() %>% 
  mutate(
    ticker = "ARCE"
  )
)

tab2 <- tab2 %>% 
  arrange(Index) %>% 
  mutate(
    Index = format(Index, "%Y"),
    yearly.returns = yearly.returns*100
    ) %>% 
  filter(Index >= "2015-01-01") %>% 
  pivot_wider(
    names_from = Index,
    values_from = yearly.returns
  ) %>% 
  rename(
    Ticker = ticker
  ) %>%
  mutate_all(~ ifelse(is.na(.), 0, .)) %>% 
  mutate(Ticker = str_squish(Ticker)) 

# Tabela 3 - Correlação dos retornos --------------------------------------

tab3 <- empresas.ce.df %>% 
  select(ref.date, ticker, ret.adjusted.prices) %>%
  mutate(
    ticker = str_remove_all(ticker, ".SA")
  ) %>% 
  pivot_wider(
    names_from = ticker,
    values_from = ret.adjusted.prices,
    values_fn = sum
  ) %>% 
  arrange(ref.date) %>%
  select(-ref.date) %>% 
  mutate_all(~ifelse(is.na(.), 0, .)) %>% 
  correlation_matrix(
    decimal.mark = ",", 
    show_significance = T,
    replace_diagonal = T,
    use = "upper"
    )


empresas.ce.df %>% 
  select(ref.date, ticker, ret.adjusted.prices) %>% 
  pivot_wider(
    names_from = ticker,
    values_from = ret.adjusted.prices,
    values_fn = sum
  ) %>% 
  arrange(ref.date) %>%
  select(-ref.date) %>% 
  mutate_all(~ifelse(is.na(.), 0, .)) %>% 
  correlation_matrix(
    show_significance = F,
    replace_diagonal = T
  ) %>% 
  as_tibble() %>%
  janitor::clean_names() %>% 
  pivot_longer(
    cols = pgmn3_sa:arce
  ) %>% 
  mutate(value = as.numeric(value)) %>% 
  drop_na(value) %>% 
  arrange(value)


my_fn <- function(data, mapping, method="lm", ...){
  p <- ggplot(data = data, mapping = mapping) + 
    geom_point() + 
    geom_smooth(method = method, ...)
  p
}

correlation_ce <- wide.empresas.ce$ret.adjusted.prices %>% 
  select(-ref.date)

g.correlation_ce <- ggpairs(
  correlation_ce,
  lower = list(
    continuous = my_fn
    ),
  showStrips = FALSE
  ) + 
  theme_test()


# Gráficos ----------------------------------------------------------------

# Todas as empresas

g1 <- empresas.ce.df %>% 
  filter(ticker != "PGMN3.SA", ticker != "COCE3.SA", ticker != "COCE6.SA") %>% 
  ggplot(aes(x = ref.date, y = price.adjusted)) + 
  geom_line() + 
  facet_wrap(~ticker, scales = "free") + 
  scale_y_continuous(
    '', 
    labels = comma_format()
  ) +
  scale_x_date(
    '',
    date_breaks = '1 years',
    date_labels =  "%Y",
    guide = guide_axis(angle = 45)
  ) + 
  theme_tq()

g2 <- empresas.ce.df %>% 
  filter(ticker == "PGMN3.SA") %>% 
  ggplot(aes(x = ref.date, y = price.adjusted)) + 
  geom_line() + 
  facet_wrap(~ticker, scales = "free") + 
  scale_y_continuous(
    '', 
    labels = comma_format()
  ) +
  scale_x_date(
    '',
    #date_breaks = "2 weeks",
    date_labels =  "%d/%b/%y"
  ) + 
  theme_tq()

g2

chart_Series(BNBR3.SA)
chartSeries(PGMN3.SA)
chartSeries(BNBR3.SA)
chartSeries(BVSP)


# Gráfico - Análise Cluster -----------------------------------------------

df <- empresas.ce.df %>% 
  select(ticker, ref.date, ret.adjusted.prices)

df_cluester <- df %>% 
  # filter(ticker != 'COCE3.SA' & ticker != 'COCE6.SA') %>% 
  arrange(ref.date) %>% 
  pivot_wider(
    names_from = ref.date,
    values_from = ret.adjusted.prices,
    values_fn = sum
  ) %>% 
  column_to_rownames(var = "ticker")

d <- dist(df_cluester, method = "euclidean")

hc1 <- hclust(d, method = "complete" )

g3 <- ggdendrogram(hc1, theme_dendro = T)


# Salvando dados ----------------------------------------------------------

"PGMN3.SA"
"BNBR3.SA"
'COCE3.SA'
'COCE5.SA'
"COCE6.SA"
'GRND3.SA'
'MDIA3.SA'
'HAPV3.SA'

write_rds(PGMN3.SA, "data/PGMN3.SA.rds")
write_rds(BNBR3.SA, "data/BNBR3.SA.rds")
# write_rds(COCE3.SA, "data/COCE3.SA.rds")
write_rds(COCE5.SA, "data/COCE5.SA.rds")
# write_rds(COCE6.SA, "data/COCE6.SA.rds")
write_rds(GRND3.SA, "data/GRND3.SA.rds")
write_rds(MDIA3.SA, "data/MDIA3.SA.rds")
write_rds(MDIA3.SA, "data/MDIA3.SA.rds")

# tabelas
empresas_selecionadas_cvm
write_rds(empresas_selecionadas_cvm, "tabs/tab.rds")
write_rds(tab1, "tabs/tab1.rds")
write_rds(tab2, "tabs/tab2.rds")
write_rds(tab3, "tabs/tab3.rds")

# Gráficos

write_rds(g1, "figs/g1.rds")
write_rds(g2, "figs/g2.rds")
write_rds(g3, "figs/g3.rds")
write_rds(g.correlation_ce, "figs/g4.rds")

