# Limpando ambiente -------------------------------------------------------

rm(list = ls())
graphics.off()

# Pacotes -----------------------------------------------------------------

library(zoo)
library(tidyverse)
library(BatchGetSymbols)
library(lubridate)
library(scales)
library(GetBCBData)
library(writexl)
library(IndexNumR)
library(tidyquant)
library(GetFREData)
library(bizdays)

# Parâmetros --------------------------------------------------------------

# Parâmetros para o download das cotações e período base do índice

inicial.date <- ymd('2010-01-01')
inicial.date_indice <- ymd('2012-01-02')
final.date <- today()

# dias últeis após o IPO para a entrada no índice

data_inclusao <- 0

# Periodo de defasagem para as tabelas

lag_tab_stat <- 48

# Parâmetros fixos - Não alterar

tresh_bad_data <- 0.001
cache_folder <- 'data/calc_indice'
cacho_folder_fre <- 'data/fre_data'
retorno <- "log"
periodicidade <- "daily"

std_peso = 0.05 # Peso padrão para o cálculo dos índices

## Dados enpresas estrangeiras

international_ticker.ce <- c('ARCE')

qtd_acoes_internacionais <- tibble(
  dt_refer = ymd(20200101),
  dt_receb = ymd(20201214),
  versao = 1,
  cd_cvm = 1,
  ticker = 'ARCE',
  denom_cia = 'Arco Platform Limited',
  free_float = 29857074,
  qtd_total = 30180000
)

## Dados empresas nacionais

cdg_empresas.df <- tribble(
  ~cd_cvm, ~ticker, ~stock_class,
  #-------/--------/-----------/
  22608, 'PGMN3.SA', 'ON', # Pague Menos
  1228, 'BNBR3.SA', 'ON', # Banco do Nordeste
  14869, 'COCE3.SA', 'ON', # Coelce
  14869, 'COCE5.SA', 'PN', # Coelce
  19615, 'GRND3.SA', 'ON', # Grendene
  20338, 'MDIA3.SA', 'ON', # M. Dias Branco
  24392, 'HAPV3.SA', 'ON', # Hapvida
  25283, 'AERI3.SA', 'ON' # Aeris
)

# Importação - FRE data ---------------------------------------------------

info_fre <- GetFREData::get_fre_links(
  companies_cvm_codes = cdg_empresas.df$cd_cvm,
  first_year = 2020,
  cache_folder = cacho_folder_fre
)

info_fre <- info_fre %>%
  select(DT_REFER, VERSAO, CD_CVM, DT_RECEB)


l_fre <- get_fre_data(companies_cvm_codes = cdg_empresas.df$cd_cvm,
                      fre_to_read = 'all',
                      first_year = 2020,
                      last_year = 2020,
                      cache_folder = cacho_folder_fre)

## Extraindo informações da lista fre

df_capital <- l_fre$df_capital

df_capital <- df_capital %>%
  left_join(info_fre) %>%
  relocate(DT_RECEB, .after = DT_REFER)

df_stockholders <- l_fre$df_stockholders

df_stockholders <- df_stockholders %>%
  left_join(info_fre) %>%
  relocate(DT_RECEB, .after = DT_REFER)

## Quantidade total de papéis em circulação

qtde_acoes <- df_stockholders %>%
  janitor::clean_names() %>%
  select(
    dt_refer,
    dt_receb,
    versao,
    cd_cvm,
    denom_cia,
    type_register,
    type_stockholder,
    qtd_ord_shares,
    qtd_pref_shares,
    controlling_stockholder
  ) %>%
  as_tibble()

qtde_acoes_total <- qtde_acoes %>%
  filter(type_register == 'Total') %>%
  select(-type_register, -type_stockholder, -controlling_stockholder) %>%
  rename(ON = qtd_ord_shares, PN = qtd_pref_shares) %>%
  pivot_longer(
    ON:PN,
    names_to = 'stock_class',
    values_to = 'qtde'
  ) %>%
  filter(qtde > 0) %>%
  left_join(cdg_empresas.df, by = c('cd_cvm', 'stock_class')) %>%
  relocate(ticker, .after = cd_cvm) %>%
  mutate(qtde = as.numeric(qtde))

## Quantidade de papéis em circulação por classe de ação

qtde_acoes_class <- df_capital %>%
  janitor::clean_names() %>%
  left_join(cdg_empresas.df, by = c('cd_cvm', 'stock_type' = 'stock_class')) %>%
  fill(ticker) %>%
  filter(qtd_issued > 0) %>%
  select(-cnpj_cia, -id_doc, -versao, -stock_class) %>%
  pivot_wider(
    names_from = 'stock_type',
    values_from = 'qtd_issued',
    values_fn = sum
  ) %>%
  mutate_if(is.numeric, ~ifelse(is.na(.), 0, .)) %>%
  mutate(qtde_total = ON + PN) %>%
  relocate(c(dt_refer, dt_receb, cd_cvm, ticker), .before = denom_cia)

## Calculando o Free-Float e finalizando a base

qtde_acoes.df <- qtde_acoes %>%
  filter(type_register == 'Outros') %>%
  select(-type_stockholder, -controlling_stockholder) %>%
  rename(ON = qtd_ord_shares, PN = qtd_pref_shares) %>%
  pivot_longer(ON:PN, names_to = 'stock_class') %>%
  mutate(value = as.numeric(value)) %>%
  pivot_wider(
    names_from = type_register,
    values_from = value,
    values_fn = sum
  ) %>%
  janitor::clean_names() %>%
  # mutate(free_float = total - acionista) %>%
  rename(free_float = outros) %>%
  filter(free_float > 0) %>%
  left_join(cdg_empresas.df) %>%
  # select(-acionista, -total) %>%
  relocate(ticker, .after = cd_cvm) %>%
  left_join(qtde_acoes_total) %>%
  select(-stock_class) %>%
  rename(qtd_total = qtde)

qtde_acoes.df <- qtde_acoes.df %>% bind_rows(qtd_acoes_internacionais)

# Importação - Calendário ANBIMA ------------------------------------------

data(holidaysANBIMA, package = 'bizdays')

cal <- create.calendar(
  "Brazil/ANBIMA",
  holidays = holidaysANBIMA,
  weekdays = c("saturday", "sunday")
)

add.bizdays(ymd(20180926), 30, cal)

# Importação - Taxa de Câmbio ---------------------------------------------

# Dowload taxa de câmbio

## Dados diários

cambio <- GetBCBData::gbcbd_get_series(
  c('cambio' = 1),
  inicial.date,
  final.date,
  cache.path = "data/cambio_ptax",
  format.data = "wide"
)

cambio <- cambio %>%
  rename(date = ref.date) %>%
  as_tibble()

# Importação - Empresas Nacionais -----------------------------------------

# Empresas nacionais

empresas.ce <- BatchGetSymbols(
  tickers = cdg_empresas.df$ticker,
  first.date = inicial.date,
  last.date = final.date,
  thresh.bad.data = tresh_bad_data,
  cache.folder = cache_folder,
  bench.ticker = '^BVSP',
  type.return = retorno,
  freq.data = periodicidade
)

# Criando DataFrame

empresas_ce_df <- empresas.ce$df.tickers

empresas_ce_nacionais_brutos <- empresas_ce_df %>%
  as_tibble() %>%
  select(ticker, ref.date, volume, price.close)  %>%
  rename(date = ref.date) %>%
  arrange(date)

# Repetindo valores faltantes

## Preços de fechamento

empresas_ce_nacionais_pc <- empresas_ce_nacionais_brutos %>%
  select(-volume) %>%
  pivot_wider(
    names_from = ticker,
    values_from = price.close,
    values_fn = sum
  ) %>%
  fill(BNBR3.SA) %>%
  fill(COCE5.SA) %>%
  fill(GRND3.SA) %>%
  fill(MDIA3.SA) %>%
  pivot_longer(
    !date,
    names_to = 'ticker',
    values_to = 'price.close'
  ) %>%
  drop_na(price.close)

## Volumes

empresas_ce_nacionais_vl <- empresas_ce_nacionais_brutos %>%
  select(-price.close) %>%
  pivot_wider(
    names_from = ticker,
    values_from = volume,
    values_fn = sum
  ) %>%
  fill(BNBR3.SA) %>%
  fill(COCE5.SA) %>%
  fill(GRND3.SA) %>%
  fill(MDIA3.SA) %>%
  pivot_longer(
    !date,
    names_to = 'ticker',
    values_to = 'volume'
  ) %>%
  drop_na(volume)

## Consolidando base

empresas_ce_nacionais <-  empresas_ce_nacionais_vl %>%
  left_join(empresas_ce_nacionais_pc, by = c('date', 'ticker'))

# Importação - Empresas Internacionais ------------------------------------

# Download do Benchmark para as empresas internacionais

sp500 <- BatchGetSymbols(
  '^GSPC',
  first.date = inicial.date,
  last.date = final.date,
  thresh.bad.data = tresh_bad_data,
  type.return = retorno,
  cache.folder = cache_folder,
  freq.data = periodicidade
)

sp500_df <- sp500$df.tickers

# Empresas internacionais

empresas.ce_internacional <- BatchGetSymbols(
  tickers = international_ticker.ce,
  first.date = inicial.date,
  last.date = final.date,
  thresh.bad.data = tresh_bad_data,
  cache.folder = cache_folder,
  bench.ticker = '^BVSP',
  type.return = retorno,
  freq.data = periodicidade
)

empresas_internacionais_ce <- empresas.ce_internacional$df.tickers

# Consolidando base de dados

empresas_internacionais_ce_df <- empresas_internacionais_ce %>%
  select(ticker, ref.date, volume, price.close) %>%
  rename(date = ref.date) %>%
  left_join(cambio, by = "date") %>%
  fill(cambio) %>%
  mutate(
    price.close = price.close * cambio
  )  %>%
  select(ticker, date, volume, price.close) %>%
  as_tibble() %>%
  arrange(date)

# Importação - Ibovespa ---------------------------------------------------

# Importação do Ibovespa

ibovespa <- BatchGetSymbols(
  '^BVSP',
  first.date = inicial.date,
  last.date = final.date,
  thresh.bad.data = tresh_bad_data,
  type.return = retorno,
  cache.folder = cache_folder,
  freq.data = periodicidade
)

ibovespa_df <- ibovespa$df.tickers

ibovespa_df <- ibovespa_df %>%
  select(
    ticker,
    ref.date,
    volume,
    price.close
  ) %>%
  rename(
    date = ref.date
  )

ibovespa_df$ticker[ibovespa_df$ticker == "^BVSP"] <- "Ibovespa"

# Consolidando Base de dados - Empresas Cearenses -------------------------

empresas_ce_nacionais <- empresas_ce_nacionais %>%
  bind_rows(ibovespa_df) %>%
  arrange(date)

# Repetindo valores faltantes

## Preços de fechamento

empresas_cp_df <- empresas_ce_nacionais %>%
  select(-volume) %>%
  pivot_wider(
    names_from = ticker,
    values_from = price.close,
    values_fn = sum
  ) %>%
  fill(Ibovespa) %>%
  pivot_longer(
    !date,
    names_to = 'ticker',
    values_to = 'price.close'
  ) %>%
  drop_na(price.close)

## Volumes

empresas_vl_df <- empresas_ce_nacionais %>%
  select(-price.close) %>%
  pivot_wider(
    names_from = ticker,
    values_from = volume,
    values_fn = sum
  ) %>%
  fill(Ibovespa) %>%
  pivot_longer(
    !date,
    names_to = 'ticker',
    values_to = 'volume'
  ) %>%
  drop_na(volume)

# Juntando bases

empresas_ce_nacionais <-  empresas_vl_df %>%
  left_join(empresas_cp_df, by = c('date', 'ticker'))

# Consolidando base com as empresas internacionais

empresas_df <- empresas_ce_nacionais %>%
  bind_rows(empresas_internacionais_ce_df) %>%
  arrange(date)

# Retirando dados faltantes e datas comemorativas -------------------------

# Repetindo valores faltantes e retirando datas comemorativas

empresas_df <- empresas_df %>%
  select(-price.close) %>%
  pivot_wider(
    names_from = ticker,
    values_from = volume,
    values_fn = sum
  ) %>%
  drop_na(Ibovespa) %>%
  fill(HAPV3.SA) %>%
  fill(ARCE) %>%
  mutate_if(is.numeric, ~ifelse(is.na(.), 0, .)) %>%
  pivot_longer(
    !date,
    names_to = 'ticker',
    values_to = 'volume'
  ) %>%
  left_join(
    empresas_df %>%
      select(-volume) %>%
      pivot_wider(
        names_from = ticker,
        values_from = price.close,
        values_fn = sum
      ) %>%
      drop_na(Ibovespa) %>%
      fill(HAPV3.SA) %>%
      fill(ARCE) %>%
      mutate_if(is.numeric, ~ifelse(is.na(.), 0, .)) %>%
      pivot_longer(
        !date,
        names_to = 'ticker',
        values_to = 'price.close'
      ),
    by = c('date', 'ticker')
  )

# Consolidando Base de Dados ----------------------------------------------

## Valor de mercado diário

base <- empresas_df %>%
  filter(ticker != 'Ibovespa', date != '2018-04-25') %>%
  select(-volume) %>%
  left_join(
    qtde_acoes.df %>%
      group_by(ticker) %>%
      filter(versao == max(versao)) %>%
      ungroup() %>%
      select(ticker, free_float, qtd_total),
    by = 'ticker'
  ) %>%
  mutate(
    free_float = ifelse(price.close == 0, 0, free_float),
    qtd_total = ifelse(price.close == 0, 0, qtd_total),
    iwf = free_float/qtd_total,
    iwf = ifelse(is.nan(iwf), 0, iwf),
    vl_merc_ff = price.close * free_float
  ) %>%
  ungroup()

## Identificando data inicial para cada ticker e entrada no índice

data_entrada <- base %>%
  select(date, ticker, price.close) %>%
  filter(price.close > 0) %>%
  group_by(ticker) %>%
  mutate(data_ipo = first(date)) %>%
  filter(date == last(date)) %>%
  ungroup() %>%
  select(-price.close) %>%
  mutate(data_entrada = add.bizdays(data_ipo, data_inclusao, cal))


## Testando entrada das empresas no índice

base <- base %>%
  left_join(
    data_entrada %>% select(-date),
    by = 'ticker'
  ) %>%
  group_by(ticker) %>%
  filter(date >= data_entrada) %>%
  ungroup() %>%
  relocate(c(data_ipo, data_entrada), .after = date)

## Valor de mercado diário

vl_merc <- base %>%
  group_by(date) %>%
  summarise(vl_merc = sum(vl_merc_ff)) %>%
  ungroup() %>%
  filter(date >= add.bizdays(inicial.date_indice, -2, cal))

## Valor de mercado mensal

vl_mercado_empresas_mensal <- vl_merc %>%
  group_by(month = month(date), year = year(date)) %>%
  slice(which.max(day(date))) %>%
  ungroup()

# Cálculo do índice -------------------------------------------------------

## Calculando redutor

redutor <- vl_merc %>%
  mutate(cmv = vl_merc/lag(vl_merc)) %>%
  filter(!is.na(cmv)) %>%
  mutate(
    data_ipo = ifelse(date %in% data_entrada$data_entrada, 1, 0),
    indice = (vl_merc/first(vl_merc)) * 100,
    redutor_inicial = vl_merc/indice
  ) %>%
  filter(data_ipo == 1) %>%
  mutate(
    redutor = cumprod(cmv) * ifelse(is.na(lag(redutor_inicial)), redutor_inicial, lag(redutor_inicial))
  ) %>%
  select(date, redutor)

## Calculando índice

indice <- vl_merc %>%
  filter(date >= inicial.date_indice) %>%
  left_join(redutor, by = 'date') %>%
  fill(redutor) %>%
  mutate(
    indice_viesado = vl_merc/first(vl_merc),
    redutor_inicial = vl_merc/indice_viesado,
    redutor = ifelse(is.na(redutor), redutor_inicial/100, redutor)
    ) %>%
  select(-redutor_inicial, -indice_viesado) %>%
  mutate(indice = vl_merc/redutor)

# ibovespaXindice - Base diária -------------------------------------------

# Índice de valor

indice <- indice %>%
  left_join(
    empresas_df %>%
      filter(ticker == "Ibovespa"),
    by = "date"
  ) %>%
  select(-volume, -ticker) %>%
  rename(ibovespa = price.close) %>%
  mutate(
    var_diaria_indice = (indice / lag(indice) - 1),
    var_52_semanas_indice = (indice / lag(indice, 52) - 1),
    ibovespa = (ibovespa / dplyr::first(ibovespa)) * 100,
    var_diaria_ibovespa = (ibovespa / lag(ibovespa) - 1),
    var_52_semanas_ibovespa = (ibovespa / lag(ibovespa, 52) - 1)
  ) %>%
  relocate(var_diaria_ibovespa, .after = var_diaria_indice)

# ibovespaXindice - Base mensal -------------------------------------------

indice_mensal <- indice %>%
  select(date, vl_merc, indice, ibovespa) %>%
  filter(!(date > inicial.date_indice & date < '2012-02-29')) %>%
  group_by(month = month(date), year = year(date)) %>%
  slice(which.max(day(date))) %>%
  ungroup() %>%
  arrange(date) %>%
  select(-month, -year) %>%
  arrange(date) %>%
  mutate(
    date = ymd(paste0(str_sub(date, end = 7), "01")),
    var_mensal_indice = (indice/lag(indice) - 1),
    acum_12_indice = (indice/lag(indice, 12) - 1),
    acum_24_indice = (indice/lag(indice,24) - 1),
    acum_48_indice = (indice/lag(indice,48) - 1),
    ibovespa = (ibovespa/dplyr::first(ibovespa))*100,
    var_mensal_ibovespa = (ibovespa/lag(ibovespa) - 1),
    acum_12_ibovespa = (ibovespa/lag(ibovespa, 12) - 1),
    acum_24_ibovespa = (ibovespa/lag(ibovespa, 24) - 1),
    acum_48_ibovespa = (ibovespa/lag(ibovespa, 48) - 1)
  )

# Cálculo para o índice de valor

tx_variacao_valor <- indice_mensal %>%
  select(date, var_mensal_indice, var_mensal_ibovespa) %>%
  drop_na() %>%
  mutate(
    var_mensal_indice = 1 + var_mensal_indice,
    var_mensal_ibovespa = 1 + var_mensal_ibovespa
  ) %>%
  group_by(year(date)) %>%
  summarise(
    acum_ano_indice = cumprod(var_mensal_indice) - 1,
    acum_ano_ibovespa = cumprod(var_mensal_ibovespa) - 1
  ) %>%
  ungroup() %>%
  janitor::clean_names()

data_tx_variacao <- indice_mensal %>%
  drop_na(var_mensal_indice) %>%
  pull(date)

tx_variacao_valor$date <- data_tx_variacao


tx_variacao_valor <- tx_variacao_valor %>%
  select(-year_date) %>%
  relocate(date)

indice_mensal <- indice_mensal %>%
  left_join(tx_variacao_valor, by = 'date') %>%
  relocate(var_mensal_ibovespa, .after = var_mensal_indice) %>%
  relocate(
    c(
      acum_12_indice,
      acum_12_ibovespa,
      acum_24_indice,
      acum_24_ibovespa,
      acum_48_indice,
      acum_48_ibovespa
    ),
    .after = last_col()
  )

# Tabela Estatística das empresas e do índice -----------------------------

filter_date_48 <- (
  base %>%
    slice_max(date) %>%
    pull(date) %>% unique()
  ) - months(lag_tab_stat - 1)

filter_date_48 <- format(filter_date_48, '%Y-%m-01')


retorno_mensal <- base %>%
  filter(date >= filter_date_48) %>%
  select(date, ticker, price.close) %>%
  bind_rows(
    empresas_df %>%
      select(date, ticker, price.close) %>%
      filter(ticker == 'Ibovespa', date >= filter_date_48)
  ) %>%
  arrange(date) %>%

  # select(-volume) %>%
  group_by(ticker) %>%
  tq_transmute(
    select = price.close,
    mutate_fun = periodReturn,
    period = "monthly",
    col_rename = "return.price.close"
  ) %>%
  drop_na(return.price.close) %>%
  filter(return.price.close != Inf)

retorno_anual <- base %>%
  filter(date >= filter_date_48) %>%
  select(date, ticker, price.close) %>%
  bind_rows(
    empresas_df %>%
      select(date, ticker, price.close) %>%
      filter(ticker == 'Ibovespa', date >= filter_date_48)
  ) %>%
  arrange(date) %>%
  # select(-volume) %>%
  group_by(ticker) %>%
  tq_transmute(
    select = price.close,
    mutate_fun = periodReturn,
    period = "yearly",
    col_rename = "return.price.close"
  ) %>%
  drop_na(return.price.close) %>%
  filter(return.price.close != Inf)


tab_stats_empresas <- retorno_mensal %>%
  tq_performance(
    Ra = return.price.close,
    Rb = NULL,
    performance_fun = table.Stats
  )

tab_stats_empresas <- tab_stats_empresas %>%
  select(
    ticker,
    ArithmeticMean,
    Stdev,
    Variance,
    Observations,
    Maximum,
    Minimum
  ) %>%
  mutate(
    CV = Stdev/ArithmeticMean,
    .after = Stdev
  )

# Consolidando Tabela com Estatísticas

tab_stats <- indice %>%
  tq_transmute(
    select = indice,
    mutate_fun = periodReturn,
    period = "monthly",
    col_rename = "return.indice"
  ) %>%
  filter(date >= filter_date_48) %>%
  tq_performance(
    Ra = return.indice,
    Rb = NULL,
    performance_fun = table.Stats
  ) %>%
  select(
    ArithmeticMean,
    Stdev,
    Variance,
    Observations,
    Maximum,
    Minimum
  ) %>%
  mutate(
    ticker = "indice",
    CV = Stdev / ArithmeticMean,
    .after = Stdev
  ) %>%
  relocate(ticker) %>%
  bind_rows(tab_stats_empresas) %>%
  mutate(
    ticker = factor(
      ticker,
      levels = c(
        "Ibovespa",
        "indice",
        "BNBR3.SA",
        'COCE3.SA',
        "COCE5.SA",
        "GRND3.SA",
        "MDIA3.SA",
        "HAPV3.SA",
        "ARCE",
        "PGMN3.SA",
        'AERI3.SA'
      )
    )
  ) %>%
  arrange(ticker)

# Beta das Empresas e do Índice -------------------------------------------

tickers_empresas_recentes <- c('PGMN3.SA', 'AERI3.SA')

# Download dos dados da Pague menos separados

empresas_recentes <- tq_get(tickers_empresas_recentes)

# Calculando o retorno do Ibovespa

ret_benc <- retorno_mensal %>%
  filter(ticker == 'Ibovespa') %>%
  pivot_wider(
    names_from = ticker,
    values_from = return.price.close
  ) %>%
  rename(Rb = Ibovespa)

# Juntando retorno mensal das empresas e do índice

RaRb <- indice %>%
  tq_transmute(
    select = indice,
    mutate_fun = periodReturn,
    period = "monthly",
    col_rename = "return.price.close"
  ) %>%
  mutate(ticker = 'indice', .before = date) %>%
  filter(date >= filter_date_48) %>%
  bind_rows(
    retorno_mensal %>%
      filter(ticker != 'Ibovespa')
  ) %>%
  rename(Ra = return.price.close) %>%
  left_join(ret_benc, by = 'date') %>%
  group_by(ticker)

# Calculando Beta dos índices e das empresas

RaRb_capm <- RaRb %>%
  filter(!(ticker %in% tickers_empresas_recentes)) %>%
  tq_performance(
    Ra = Ra,
    Rb = Rb,
    performance_fun = table.CAPM
  ) %>%
  select(
    ticker,
    AnnualizedAlpha,
    Alpha,
    Beta,
    Correlation,
    `Correlationp-value`,
    `R-squared`
  ) %>% ungroup()

# Modificando nome para indicar Benchmark

RaRb_capm$ticker[RaRb_capm$ticker == "ARCE"] <- "ARCE_Ibovespa"

## CAPM das empresas adicionadas recentemente


### Pague menos

capm_empresas_recentes_pgmn3 <- empresas_recentes %>%
  select(date, symbol, close) %>%
  group_by(symbol) %>%
  tq_transmute(
    select = close,
    mutate_fun = periodReturn,
    period = "monthly",
    col_rename = "Ra"
  ) %>%
  left_join(ret_benc, by = 'date') %>%
  filter(symbol == 'PGMN3.SA') %>%
  tq_performance(Ra = Ra,
                 Rb = Rb,
                 performance_fun = table.CAPM) %>%
  mutate(ticker = 'PGMN3.SA') %>%
  relocate(ticker) %>%
  select(
    ticker,
    AnnualizedAlpha,
    Alpha,
    Beta,
    Correlation,
    `Correlationp-value`,
    `R-squared`
  ) %>%
  ungroup() %>%
  select(-symbol)

# Calculando Beta das empresas internacionais

## Retorno mensal

ret_mensal_emp_int <- empresas_internacionais_ce %>%
  select(ticker, ref.date, price.close) %>%
  rename(date = ref.date) %>%
  group_by(ticker) %>%
  tq_transmute(
    select = price.close,
    mutate_fun = periodReturn,
    period = "monthly",
    col_rename = "Ra"
  ) %>%
  left_join(
    sp500_df %>%
      select(ticker, ref.date, price.close) %>%
      rename(date = ref.date) %>%
      tq_transmute(
        select = price.close,
        mutate_fun = periodReturn,
        period = "monthly",
        col_rename = "Rb"
      ),
    by = "date"
  )


RaRb_capm_int <- ret_mensal_emp_int %>%
  tq_performance(
    Ra = Ra,
    Rb = Rb,
    performance_fun = table.CAPM
  ) %>%
  select(
    ticker,
    AnnualizedAlpha,
    Alpha,
    Beta,
    Correlation,
    `Correlationp-value`,
    `R-squared`
  ) %>%
  ungroup()

# Modificando nome para indicar Benchmark

RaRb_capm_int$ticker[RaRb_capm_int$ticker == "ARCE"] <- "ARCE_SP500"

# Consolidando Tabela

RaRb_capm <- RaRb_capm %>%
  bind_rows(RaRb_capm_int) %>%
  bind_rows(capm_empresas_recentes_pgmn3)



# Tabela com Retorno das empresas -----------------------------------------

# Calculando retorno da Pague menos

ret_empresas_recentes <- empresas_recentes %>%
  select(date, symbol, close) %>%
  group_by(symbol) %>%
  tq_transmute(
    select = close,
    mutate_fun = periodReturn,
    period = 'monthly',
    col_rename = 'retorno_mensal'
  ) %>%
  ungroup() %>%
  rename(ticker = symbol) %>%
  left_join(
    empresas_recentes %>%
      select(date, symbol, close) %>%
      group_by(symbol) %>%
      tq_transmute(
        select = close,
        mutate_fun = periodReturn,
        period = 'yearly',
        col_rename = 'retorno_anual'
      ) %>%
      ungroup() %>%
      rename(ticker = symbol),
    by = c('date', 'ticker')
  ) %>%
  slice_max(order_by = date)

# Consolidando Tabela

tab_ret_emp <- retorno_mensal %>%
  ungroup() %>%
  filter(!ticker %in% tickers_empresas_recentes) %>%
  pivot_wider(
    names_from = ticker,
    values_from = return.price.close,
    values_fn = sum
  ) %>%
  tail(1) %>%
  pivot_longer(
    !date,
    names_to = 'ticker',
    values_to = 'retorno_mensal'
  ) %>%
  left_join(
    retorno_anual %>%
      ungroup() %>%
      filter(!ticker %in% tickers_empresas_recentes) %>%
      pivot_wider(
        names_from = ticker,
        values_from = return.price.close,
        values_fn = sum
      ) %>%
      tail(1) %>%
      pivot_longer(
        !date,
        names_to = 'ticker',
        values_to = 'retorno_anual'
      ),
    by = c('date', 'ticker')
  ) %>%
  bind_rows(ret_empresas_recentes) %>%
  bind_rows(
    indice_mensal %>%
      select(var_mensal_indice, acum_ano_indice) %>%
      tail(1) %>%
      mutate(ticker = 'indice') %>%
      relocate(ticker) %>%
      rename(
        retorno_mensal = var_mensal_indice,
        retorno_anual = acum_ano_indice
      )
  ) %>%
  fill(date) %>%
  mutate(
    ticker = factor(
      ticker,
      levels = c(
        "Ibovespa",
        "indice",
        "BNBR3.SA",
        'COCE3.SA',
        "COCE5.SA",
        "GRND3.SA",
        "MDIA3.SA",
        "HAPV3.SA",
        "ARCE",
        "PGMN3.SA",
        'AERI3.SA'
      )
    )
  ) %>%
  arrange(ticker)



# Consolidando informações ------------------------------------------------

## Contribuição de cada empresa no índice

base_contribuicao <- base %>%
  filter(date >= inicial.date_indice) %>%
  select(date, ticker, free_float, vl_merc_ff) %>%
  group_by(date) %>%
  mutate(contribuicao = vl_merc_ff/sum(vl_merc_ff)) %>%
  ungroup() %>%
  select(-vl_merc_ff) %>%
  left_join(
    indice %>%
      select(date, redutor),
    by = 'date'
  )

contribuicao <- slice_max(base_contribuicao, order_by = date) %>%
  arrange(desc(contribuicao))

## Base diária Wide

base.wide <- base %>%
  select(date, ticker, price.close) %>%
  filter(date >= inicial.date_indice) %>%
  pivot_wider(
    names_from = ticker,
    values_from = price.close
  ) %>%
  arrange(date)

base_wide_price_mensal <- base.wide %>%
  group_by(month = month(date), year = year(date)) %>%
  slice(which.max(day(date))) %>%
  ungroup() %>%
  arrange(date) %>%
  select(-month, -year)

## Base mensal long

base_mensal <- base %>%
  group_by(month = month(date), year = year(date)) %>%
  slice(which.max(day(date))) %>%
  ungroup() %>%
  arrange(date) %>%
  select(-month, -year) %>%
  select(-data_ipo, -data_entrada, -iwf, -vl_merc_ff) %>%
  filter(date >= inicial.date_indice)

# Save Data ---------------------------------------------------------------

write_xlsx(
  list(
    'tab1_retorno_empresas' = tab_ret_emp,
    'tab2_risco_empresas' = tab_stats,
    'tab3_risco_sistemico' = RaRb_capm,
    'indice' = indice,
    'indice_mensal' = indice_mensal,
    'contribuicao' = contribuicao,
    'base_contribuicao' = base_contribuicao,
    "base" = base %>% filter(date >= inicial.date_indice),
    'base_mensal' = base_mensal,
    "base_wide_price" = base.wide,
    'base_wide_price_mensal' = base_wide_price_mensal,
    'data_inclusao' = data_entrada,
    "cambio_PTAX" = cambio %>% rename(cambio_ptax = cambio) %>% filter(date >= inicial.date_indice)
  ),
  "out/indice_nupe.xlsx"
)
