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

# Parâmetros para o download das cotações

inicial.date <- ymd('2010-01-01')
inicial.date_indice <- ymd('2011-12-29')
final.date <- today()
tresh_bad_data <- 0.001
cache_folder <- 'data/calc_indice'
retorno <- "log"
periodicidade <- "daily"

std_peso = 0.05 # Peso padrão para o cálculo dos índices

international_ticker.ce <- c('ARCE')

tickers.ce <- c(
  "PGMN3.SA",
  "BNBR3.SA",
  'COCE5.SA',
  'GRND3.SA',
  'MDIA3.SA',
  'HAPV3.SA'
)

# Código das empresas para download do relatório FRE

cdg_empresas <- c(
  22608, # Pague menos
  1228, # Banco do Nordeste
  14869, # Coelce
  19615, # Grendene
  20338, # M. Dias Branco
  24392 # Hapvida
)

# Importação - FRE data ---------------------------------------------------

l_fre <- get_fre_data(companies_cvm_codes = cdg_empresas,
                      fre_to_read = 'last',
                      first_year = 2020,
                      last_year = 2020,
                      cache_folder = 'data/fre_data')


df_stockholders <- l_fre$df_stockholders

qtde_acoes <- df_stockholders %>%
  janitor::clean_names() %>%
  select(
    dt_refer,
    cd_cvm,
    denom_cia,
    type_register,
    type_stockholder,
    qtd_ord_shares,
    qtd_pref_shares,
    controlling_stockholder
  ) %>%
  as_tibble()

qtde_acoes.df <- qtde_acoes %>%
  filter(type_register %in% c('Total', 'AcoesTesouraria')) %>%
  mutate(
    ticker = case_when(
      cd_cvm == 1228 ~ 'BNBR3.SA',
      cd_cvm == 14869 ~ 'COCE5.SA',
      cd_cvm == 22608 ~ 'PGMN3.SA',
      cd_cvm == 19615 ~ 'GRND3.SA',
      cd_cvm == 24392 ~ 'HAPV3.SA',
      cd_cvm == 20338 ~ 'MDIA3.SA'
    ),
    .before = denom_cia
  ) %>%
  select(
    # -type_register,
    -denom_cia,
    -type_stockholder,
    -controlling_stockholder,
    -cd_cvm
  ) %>%
  pivot_longer(
    qtd_ord_shares:qtd_pref_shares,
    values_to = 'qtde_acoes'
  ) %>%
  filter(name == 'qtd_ord_shares') %>%
  select(-name) %>%
  rename(date = dt_refer) %>%
  mutate(qtde_acoes = as.numeric(qtde_acoes)) %>%
  pivot_wider(
    names_from = type_register,
    values_from = qtde_acoes,
    values_fn = sum
  ) %>%
  mutate(
    qtde_acoes = Total - AcoesTesouraria
  ) %>%
  select(-Total, -AcoesTesouraria) %>%
  add_row(
    date = ymd(20200101),
    ticker = 'ARCE',
    qtde_acoes = 54939088
  )

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
  tickers = tickers.ce,
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






# Cálculo dos pesos -------------------------------------------------------

# Calculando os volumes mensais

volumes <- empresas_df %>%
  filter(ticker != "Ibovespa") %>%
  select(-price.close) %>%
  mutate(
    date = ymd(
      paste0(str_sub(date, end = 7), "-01")
    )
  ) %>%
  group_by(date, ticker) %>%
  summarise(volume = sum(volume)) %>%
  ungroup() %>%
  pivot_wider(
    names_from = ticker,
    values_from = volume
  )

# Calculando volume dos últimos 12 meses

volumes_12_meses <- volumes %>%
  summarise_if(
    is.numeric,
    ~rollsum(x = ., k = 12, na.pad = T, align = "right")
  )

# Adicionando data e retirando valores faltantes

volumes_12_meses_df <- volumes %>%
  select(date) %>%
  mutate(
    mes = lubridate::month(date)
  ) %>%
  bind_cols(volumes_12_meses) %>%
  # filter(mes %in% c(3, 6, 9, 12)) %>%
  drop_na() %>%
  select(-mes)

# Calculando pesos sem padronização

pesos_variaveis_brutos <- volumes_12_meses_df %>%
  janitor::adorn_totals(where = "col") %>%
  janitor::adorn_percentages() %>%
  select(-Total) %>% as_tibble() %>%
  pivot_longer(
    !date,
    names_to = "ticker",
    values_to = 'peso'
  ) %>%
  mutate(
    ano = year(date),
    mes = month(date)
  ) %>%
  relocate(c(ano, mes), .after = date) %>%
  select(-date)

# Consolidando pesos variáveis por ticker

pesos_variaveis.wide <- empresas_df %>%
  filter(ticker != "Ibovespa") %>%
  mutate(
    ano = year(date),
    mes = month(date)
  ) %>%
  relocate(c(ano, mes), .after = date) %>%
  left_join(pesos_variaveis_brutos, by = c("ano", 'mes', 'ticker')) %>%
  select(date, ticker, peso) %>%
  pivot_wider(
    names_from = ticker,
    values_from = peso,
    values_fn = sum
  ) %>%
  fill(where(is.numeric), .direction = "downup")

# Padronizando pesos

pesos_padronizados.wide <- pesos_variaveis.wide %>%
  mutate(
    across(
      where(is.numeric),
      ~ ifelse(. < std_peso & . > 0, std_peso, .)
    )
  ) %>%
  janitor::adorn_totals(where = "col", name = "total") %>%
  as_tibble() %>%
  mutate(
    total = total - 1
  ) %>%
  mutate(
    teste_ = across(
      where(is.numeric),
      ~ ifelse(. <= std_peso, 0, 1)
    )
  )

# Critério de divisão das empresas com valores menores que 5%.

divisao_tickers <- pesos_padronizados.wide$teste_ %>%
  select(-total) %>%
  janitor::adorn_totals(where = 'col', name = 'qtde_empresas') %>%
  select(qtde_empresas) %>%
  as_tibble()

# Consolidando pesos variáveis

pesos_padronizados_df.wide <- pesos_padronizados.wide %>%
  select(-teste_) %>%
  bind_cols(divisao_tickers) %>%
  mutate(total = total/qtde_empresas) %>%
  select(-qtde_empresas) %>%
  mutate(
    across(
      where(is.numeric),
      ~ifelse(. <= std_peso + 0.015 | . == 0, ., . - total)
    )
  ) %>%
  select(-total)

pesos_variaveis <- pesos_padronizados_df.wide %>%
  pivot_longer(
    cols = where(is.numeric),
    names_to = "ticker",
    values_to = "peso"
  )

# selecionando pesos atuais

pesos <- pesos_padronizados_df.wide %>%
  mutate_all(last) %>%
  unique.data.frame() %>%
  pivot_longer(
    cols = !date,
    names_to = 'ticker',
    values_to = 'peso_padronizado'
  ) %>%
  mutate(
    date = ymd(paste0(str_sub(date, end = 7), '01'))
  ) %>%
  left_join(
    volumes_12_meses_df %>%
      tail(1) %>%
      pivot_longer(
        !date,
        names_to = 'ticker',
        values_to = 'volume_12_meses'
      ),
    by = c('date', 'ticker')
  )


# Consolidando base com pesos padronizados --------------------------------

empresas_ce <- empresas_df %>%
  filter(ticker != "Ibovespa") %>%
  left_join(pesos_variaveis, by = c("date", "ticker")) %>%
  select(-volume) %>%
  filter(date >= inicial.date_indice)

empresas_ce <- empresas_ce %>%
  mutate(
    peso = ifelse(
      price.close == 0 & peso == 0.05 |
        price.close == 0.05 & peso == 0 |
        price.close == 0 & peso > 0,
      NA,
      peso),
    price.close = ifelse(is.na(peso) & price.close == 0, NA, price.close)
  )


# Arrumando pesos

pesos_padronizados.wide2 <- empresas_ce %>%
  select(-price.close) %>%
  pivot_wider(
    names_from = ticker,
    values_from = peso,
    values_fn = sum
  ) %>%
  janitor::adorn_totals(where = "col", name = "total", na.rm = F) %>%
  mutate(
    total = ifelse(total > 1.01 | total < 0.98, NA, total)
  ) %>%
  as_tibble()

data_pesos <- pesos_padronizados.wide2[,1]

pesos_padronizados.wide3 <- pesos_padronizados.wide2[,-1] * pesos_padronizados.wide2$total

pesos_padronizados.wide3$date <- data_pesos

pesos_padronizados.wide3 <- pesos_padronizados.wide3 %>%
  relocate(date) %>%
  fill(where(is.numeric)) %>%
  select(-total) %>%
  pivot_longer(
    !date,
    names_to = 'ticker',
    values_to = 'peso'
  )

pesos_padronizados.wide3$date <- pesos_padronizados.wide3$date$date

empresas_ce <- empresas_ce %>%
  select(-peso) %>%
  left_join(pesos_padronizados.wide3, by = c('date', 'ticker')) %>%
  mutate(
    price.close = ifelse(is.na(price.close), 0, price.close)
    )

# Cálculo do índice -------------------------------------------------------

# Ajusntado preços de fechamento

t1 <- empresas_ce %>%
  select(-peso) %>%
  pivot_wider(
    names_from = ticker,
    values_from = price.close,
    values_fn = sum
  ) %>%
  fill(where(is.numeric)) %>%
  pivot_longer(
    cols = !date,
    names_to = "ticker",
    values_to = "price.close"
  )

# Pesos

t2 <- empresas_ce %>%
  select(-price.close) %>%
  pivot_wider(
    names_from = ticker,
    values_from = peso,
    values_fn = sum
  ) %>%
  fill(where(is.numeric)) %>%
  pivot_longer(
    cols = !date,
    names_to = "ticker",
    values_to = "peso"
  )

t <- t1  %>%
  left_join(t2, by = c("date", "ticker"))

remover_data <- t %>%
  filter(is.na(peso)) %>%
  select(date) %>%
  unique.data.frame()

remover_data <- remover_data$date

t <- t %>%
 # filter(!date %in% remover_data) %>%
  mutate(
    cdg_date = as.numeric(as_factor(as.character(date)))
  )

data_indice <- t %>%
  select(-peso, -cdg_date) %>%
  pivot_wider(
    names_from = ticker,
    values_from = price.close
  ) %>% select(date)

# Selecionando quantidade de ações em circulação

# t <- t %>%
#   left_join(
#     qtde_acoes.df %>%
#       select(-date),
#     by = 'ticker'
#   ) %>%
#   mutate(
#     qvar = qtde_acoes*peso,
#     .after = price.close
#   ) %>%
#   select(-peso, -qtde_acoes)


valor_portifolio <- values(
  t,
  pvar = "price.close",
  qvar = "peso",
  pervar = "cdg_date",
  prodID = "ticker"
)


# valor_portifolio <- priceIndex(t,
#            pvar = "price.close",
#            qvar = "peso",
#            pervar = "cdg_date",
#            prodID = "ticker",
#            indexMethod = "laspeyres",
#            output = "chained")


valor_portifolio_df <- tibble(
  date = data_indice$date,
  valor_portifolio = as.numeric(valor_portifolio)
)

valor_portifolio_df <- valor_portifolio_df %>%
  drop_na(valor_portifolio) %>%
  mutate(indice = (valor_portifolio/first(valor_portifolio))*100)

# ibovespaXindice - Base diária -------------------------------------------

# Índice de valor

valor_portifolio_df <- valor_portifolio_df %>%
  left_join(
    empresas_df %>%
      filter(ticker == 'Ibovespa')
    , by = "date") %>%
  select(-volume, -ticker) %>%
  rename(ibovespa = price.close) %>%
  mutate(
    var_diaria_indice = (indice/lag(indice) - 1),
    var_52_semanas_indice = (indice/lag(indice, 52) - 1),
    ibovespa = (ibovespa/dplyr::first(ibovespa))*100,
    var_diaria_ibovespa = (ibovespa/lag(ibovespa) - 1),
    var_52_semanas_ibovespa = (ibovespa/lag(ibovespa, 52) - 1)
  ) %>%
  relocate(var_diaria_ibovespa, .after = var_diaria_indice)


# ibovespaXindice - Base mensal -------------------------------------------

valor_portifolio_mensal_df <- valor_portifolio_df %>%
  select(date, valor_portifolio, indice, ibovespa) %>%
  group_by(month = month(date), year = year(date)) %>%
  slice(which.max(day(date))) %>%
  ungroup() %>%
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

tx_variacao_valor <- valor_portifolio_mensal_df %>%
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

data_tx_variacao <- valor_portifolio_mensal_df %>%
  drop_na(var_mensal_indice) %>%
  pull(date)

tx_variacao_valor$date <- data_tx_variacao


tx_variacao_valor <- tx_variacao_valor %>%
  select(-year_date) %>%
  relocate(date)

valor_portifolio_mensal_df <- valor_portifolio_mensal_df %>%
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

retorno_mensal <- empresas_df %>%
  filter(date >= inicial.date_indice) %>%
  select(-volume) %>%
  group_by(ticker) %>%
  tq_transmute(
    select = price.close,
    mutate_fun = periodReturn,
    period = "monthly",
    col_rename = "return.price.close"
  ) %>%
  drop_na(return.price.close) %>%
  filter(return.price.close != Inf)

retorno_anual <- empresas_df %>%
  filter(date >= inicial.date_indice) %>%
  select(-volume) %>%
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

tab_stats <- valor_portifolio_df %>%
  tq_transmute(
    select = indice,
    mutate_fun = periodReturn,
    period = "monthly",
    col_rename = "return.indice"
  ) %>%
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
        "COCE5.SA",
        "GRND3.SA",
        "MDIA3.SA",
        "HAPV3.SA",
        "ARCE",
        "PGMN3.SA"
      )
    )
  ) %>%
  arrange(ticker)

# Beta das Empresas e do Índice -------------------------------------------

# Download dos dados da Pague menos separados

pague_menos <- tq_get('PGMN3.SA')

# Calculando o retorno do Ibovespa

ret_benc <- retorno_mensal %>%
  filter(ticker == 'Ibovespa') %>%
  pivot_wider(
    names_from = ticker,
    values_from = return.price.close
  ) %>%
  rename(Rb = Ibovespa)

# Juntando retorno mensal das empresas e do índice

RaRb <- valor_portifolio_df %>%
  tq_transmute(
    select = indice,
    mutate_fun = periodReturn,
    period = "monthly",
    col_rename = "return.price.close"
  ) %>%
  mutate(ticker = 'indice', .before = date) %>%
  bind_rows(
    retorno_mensal %>%
      filter(ticker != 'Ibovespa')
  ) %>%
  rename(Ra = return.price.close) %>%
  left_join(ret_benc, by = 'date') %>%
  group_by(ticker)

# Calculando Beta dos índices e das empresas

RaRb_capm <- RaRb %>%
  filter(ticker != "PGMN3.SA") %>%
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

capm_pague_menos <- pague_menos %>%
  select(date, close) %>%
  tq_transmute(
    select = close,
    mutate_fun = periodReturn,
    period = "monthly",
    col_rename = "Ra"
  ) %>%
  left_join(ret_benc, by = 'date') %>%
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
  ungroup()

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
  bind_rows(capm_pague_menos)

# Tabela com Retorno das empresas -----------------------------------------

# Calculando retorno da Pague menos

ret_PGMN3 <- pague_menos %>%
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
    pague_menos %>%
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
  tail(1)

# Consolidando Tabela

tab_ret_emp <- retorno_mensal %>%
  ungroup() %>%
  filter(ticker != 'PGMN3.SA') %>%
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
      filter(ticker != 'PGMN3.SA') %>%
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
  bind_rows(ret_PGMN3) %>%
  bind_rows(
    valor_portifolio_mensal_df %>%
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
        "COCE5.SA",
        "GRND3.SA",
        "MDIA3.SA",
        "HAPV3.SA",
        "ARCE",
        "PGMN3.SA"
      )
    )
  ) %>%
  arrange(ticker)




# Formatando Bases para Output --------------------------------------------

# Dadods em formato wide

## Preço de fechamento

empresas_df.wide <- empresas_df  %>%
  select(-volume) %>%
  pivot_wider(
    names_from = ticker,
    values_from = price.close,
    values_fn = sum
  ) %>%
  arrange(date)

# Volume

empresas_df_diario_long <- empresas_df  %>%
  select(-price.close) %>%
  pivot_wider(
    names_from = ticker,
    values_from = volume,
    values_fn = sum
  ) %>%
  arrange(date) %>%
  pivot_longer(
    !date,
    names_to = 'ticker',
    values_to = 'volume'
  )

empresas_diario_long <- empresas_df.wide %>%
  pivot_longer(
    !date,
    names_to = 'ticker',
    values_to = 'price.close'
  ) %>%
  left_join(empresas_df_diario_long, by = c('date', 'ticker')) %>%
  filter(price.close > 0)

# Dados Mensais

## Preço de Fechamento

empresas_mensal_df <- empresas_df %>%
  select(-volume) %>%
  pivot_wider(
    names_from = ticker,
    values_from = price.close,
    values_fn = sum
  ) %>%
  group_by(month = month(date), year = year(date)) %>%
  slice(which.max(day(date))) %>%
  ungroup() %>%
  select(-month, -year) %>%
  mutate(
    date = ymd(paste0(str_sub(date, end = 7), "01"))
  ) %>%
  arrange(date)

## Volume

empresas_mensal_df_vol <- empresas_df %>%
  select(-price.close) %>%
  pivot_wider(
    names_from = ticker,
    values_from = volume,
    values_fn = sum
  ) %>%
  group_by(month = month(date), year = year(date)) %>%
  slice(which.max(day(date))) %>%
  ungroup() %>%
  select(-month, -year) %>%
  mutate(
    date = ymd(paste0(str_sub(date, end = 7), "01"))
  ) %>%
  arrange(date) %>%
  pivot_longer(
    !date,
    names_to = 'ticker',
    values_to = 'volume'
  )

empresas_mensal_long <- empresas_mensal_df %>%
  pivot_longer(
    !date,
    names_to = 'ticker',
    values_to = 'price.close'
  ) %>%
  left_join(empresas_mensal_df_vol, by = c('date', 'ticker')) %>%
  filter(price.close > 0)

# Save Data ---------------------------------------------------------------

write_xlsx(
  list(
    'tab1_retorno_empresas' = tab_ret_emp,
    'tab2_risco_empresas' = tab_stats,
    'tab3_risco_sistemico' = RaRb_capm,
    'indice_valor_portifolio' = valor_portifolio_df,
    'indice_valor_portifolio_mensal' = valor_portifolio_mensal_df,
    "base_mensal_price.close" = empresas_mensal_df %>% filter(date >= "2012-01-01"),
    "base_diaria_price.close" = empresas_df.wide %>% filter(date >= "2012-01-01"),
    'base_mensal_long' = empresas_mensal_long %>% filter(date >= "2012-01-01"),
    'base_diaria_long' = empresas_diario_long %>% filter(date >= "2012-01-01"),
    "price.closeXpesos" = empresas_ce %>% filter(peso > 0, date >= "2012-01-01"),
    "cambio_PTAX" = cambio %>% rename(cambio_ptax = cambio),
    "pesos_atuais" = pesos,
    'qtde_acoes' = qtde_acoes
  ),
  "out/indice_nupe.xlsx"
)



