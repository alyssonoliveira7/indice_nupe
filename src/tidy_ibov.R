
# Pacotes -----------------------------------------------------------------

library(readxl)
library(tidyverse)
library(lubridate)

# Importando dados --------------------------------------------------------

ibovespa <- read_excel('data/ibovespa.xlsx', sheet = 'ibovespa_diario')

ibovespa_df <- ibovespa %>%
  janitor::clean_names() %>%
  mutate(date = ymd(date)) %>%
  group_by(mes = month(date), ano = year(date)) %>%
  slice(which.max(day(date))) %>%
  ungroup() %>%
  select(-ano, -mes) %>%
  arrange(date)


ibovespa_df <- ibovespa_df %>%
  mutate(date = ymd(format(date, "%Y-%b-01"))) %>%
  select(-var_diaria_indice, -var_52_dias_ibovespa)

# Importando - empresas ---------------------------------------------------

base <- read_excel('out/indice_nupe.xlsx', sheet = 'base')


retorno_mensal <- base %>%
  select(date, ticker, price.close) %>%
  mutate(date = ymd(date)) %>%
  group_by(ticker) %>%
  tq_transmute(
    select = price.close,
    mutate_fun = periodReturn,
    period = 'monthly',
    col_rename = 'retorno_mensal'
  ) %>%
  arrange(date) %>%
  filter(date >= "2021-01-01" & date < "2021-02-01") %>%
  mutate(date = ymd(format(date, "%Y-%b-01"))) %>%
  select(-date) %>%
  ungroup()


retorno_anual <- base %>%
  select(date, ticker, price.close) %>%
  mutate(date = ymd(date)) %>%
  filter(date < "2021-02-01") %>%
  group_by(ticker) %>%
  tq_transmute(
    select = price.close,
    mutate_fun = periodReturn,
    period = 'yearly',
    col_rename = 'retorno_anual'
  ) %>%
  arrange(date) %>%
  filter(date >= "2021-01-01") %>%
  select(-date) %>%
  ungroup()

tab_retornos <- retorno_mensal %>%
  left_join(retorno_anual, by = 'ticker') %>%
  arrange(ticker)


writexl::write_xlsx(ibovespa_df, 'ibovespa.xlsx')

writexl::write_xlsx(tab_retornos, 'tab_retornos.xlsx')

