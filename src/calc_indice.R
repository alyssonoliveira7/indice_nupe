# Limpando dados ----------------------------------------------------------

rm(list = ls())
graphics.off()

# Pacotes -----------------------------------------------------------------

library(tidyverse)
library(BatchGetSymbols)
library(lubridate)
library(scales)
library(GetBCBData)
library(writexl)
library(zoo)
library(IndexNumR)
library(quantmod)

# Parâmetros --------------------------------------------------------------

# Parâmetros para o download das cotações

inicial.date <- ymd('2010-01-01')
inicial.date_indice <- ymd('2012-01-01')
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
  # 'COCE3.SA',
  'COCE5.SA',
  # "COCE6.SA",
  'GRND3.SA',
  'MDIA3.SA',
  'HAPV3.SA'
)


# Importando dados --------------------------------------------------------

# Dados diários

# Dowload taxa de câmbio

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

empresas_df <- empresas.ce$df.tickers

empresas_df <- empresas_df %>%
  as_tibble() %>%
  select(ticker, ref.date, volume, price.close)  %>%
  rename(date = ref.date) %>%
  arrange(date)

glimpse(empresas_df)

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

empresas_internacionais_ce_df <- empresas_internacionais_ce %>%
  select(ticker, ref.date, volume, price.close) %>%
  rename(date = ref.date) %>%
  left_join(cambio, by = "date") %>%
  mutate(
    price.close = price.close * cambio
  )  %>%
  select(ticker, date, volume, price.close) %>%
  as_tibble() %>%
  arrange(date)

glimpse(empresas_internacionais_ce_df)

# Dados mensais

cotacao <- quantmod::getSymbols(
  c(tickers.ce, international_ticker.ce),
  src = 'yahoo',
  from = inicial.date,
  to = final.date,
  periodicity = "daily",
  auto.assign = T,
  warnings = F
) %>%
  map(~Cl(get(.))) %>%
  reduce(merge) %>%
  `colnames<-`(c(tickers.ce, international_ticker.ce))

# Dataframe - empresas Cearenses ------------------------------------------

empresas_df <- empresas_df %>%
  bind_rows(empresas_internacionais_ce_df) %>%
  arrange(date)

# Retirando feriados

empresas_df_drop_feriados <- empresas_df %>%
  select(-volume) %>%
  pivot_wider(
    names_from = ticker,
    values_from = price.close
  ) %>%
  drop_na(MDIA3.SA)

data_indice <- empresas_df_drop_feriados$date


empresas_df <- empresas_df_drop_feriados %>%
  pivot_longer(
    !date,
    names_to = "ticker",
    values_to = "price.close"
  ) %>%
  left_join(
    empresas_df %>% select(-price.close),
    by = c("date", 'ticker')
    ) %>%
  mutate_if(is.numeric, ~ifelse(is.na(.), 0, .))

# Cálculo dos pesos -------------------------------------------------------

volumes <- empresas_df %>%
  select(-price.close) %>%
  mutate(
    date = ymd(
      paste0(str_sub(date, end = 7), "-01")
    )
  ) %>%
  mutate_if(is.numeric, ~ifelse(is.na(.), 0, .)) %>%
  group_by(ticker, date) %>%
  summarise(volume = sum(volume)) %>%
  ungroup() %>%
  pivot_wider(
    names_from = ticker,
    values_from = volume
  ) %>%
  arrange(date) %>%
  mutate_if(is.numeric, ~ifelse(is.na(.), 0, .))

v <- volumes %>%
  summarise_if(
    is.numeric,
    ~rollsum(x = ., k = 12, na.pad = T, align = "right")
  )

v1 <- volumes %>%
  select(date) %>%
  mutate(
    mes = lubridate::month(date)
  ) %>%
  bind_cols(v) %>%
  filter(mes == 6 | mes == 12) %>%
  drop_na() %>%
  select(-mes)

pesos_variaveis <- v1 %>%
  janitor::adorn_totals(where = "col") %>%
  janitor::adorn_percentages() %>%
  select(-Total) %>% as_tibble()

pesos_variaveis <- empresas_df %>%
  left_join(pesos_variaveis, by = c("date")) %>%
  arrange(date) %>%
  select(-ticker, -volume, -price.close) %>%
  fill(where(is.numeric), .direction = "downup") %>%
  unique.data.frame()

pesos_variaveis <- pesos_variaveis %>%
  pivot_longer(
    cols = where(is.numeric),
    names_to = "ticker",
    values_to = "peso"
  )

pesos <- volumes %>%
  summarise_if(
    is.numeric,
    ~rollsum(x = ., k = 12, na.pad = F, align = "right")
  ) %>%
  mutate_all(last) %>%
  unique.data.frame() %>%
  pivot_longer(
    cols = everything()
  ) %>%
  mutate(
    peso = (value/sum(value))
  )

# Padronizando pesos

x <- pesos_variaveis %>%
  pivot_wider(names_from = ticker, values_from = peso) %>%
  mutate(
    across(
      where(is.numeric),
      ~ifelse(. < std_peso & . > 0, std_peso, .)
      )
    ) %>%
  janitor::adorn_totals(where = "col", name = 'total') %>%
  as_tibble() %>%
  mutate(
    total = total - 1
  ) %>%
  mutate(
    teste_ = across(
      where(is.numeric),
      ~ifelse(. <= std_peso, 0, 1)
      )
    )


y <- x$teste_ %>%
  select(-total) %>%
  janitor::adorn_totals(where = 'col', name = 'qtde_empresas') %>%
  select(qtde_empresas) %>%
  as_tibble()


z <- x %>%
  select(-teste_) %>%
  bind_cols(y) %>%
  mutate(total = total/qtde_empresas) %>%
  select(-qtde_empresas) %>%
  mutate(
    across(
      where(is.numeric),
      ~ifelse(. <= std_peso | . == 0, ., . - total)
      )
    ) %>%
  select(-total)


pesos_variaveis <- z %>%
  pivot_longer(
    cols = where(is.numeric),
    names_to = "ticker",
    values_to = "peso"
  )

# Consolidando base -------------------------------------------------------

empresas_ce <- empresas_df %>%
  left_join(pesos_variaveis, by = c("date", "ticker")) %>%
  arrange(date) %>%
  select(-volume) %>%
  filter(date >= inicial.date_indice)

# Calculando valor do portifólio ------------------------------------------

# Transformando base em matriz

empresas_matrix <- empresas_df %>%
  select(-volume) %>%
  pivot_wider(
    names_from = ticker,
    values_from = price.close
  ) %>% arrange(date) %>%
  mutate_if(is.numeric, ~ifelse(is.na(.), 0, .)) %>%
  as.matrix()

data <- empresas_matrix[,1]

rownames(empresas_matrix) <- empresas_matrix[,1]
empresas_matrix <- empresas_matrix[,-1]

# Trnasformando pesos em matriz

pesos_matrix <- as.matrix(pesos$peso)

# Calculando Valor do portifolio

empresas_matrix <- apply(empresas_matrix, 2, as.numeric)

valor_portifolio <- empresas_matrix %*% (pesos_matrix*100)

rownames(valor_portifolio) <- data

valor_portifolio <- tibble(
  date = rownames(valor_portifolio),
  valor_portifolio = as.numeric(valor_portifolio)
)

valor_portifolio <- valor_portifolio %>%
  filter(valor_portifolio > 0) %>%
  mutate(
    var_mensal = valor_portifolio/lag(valor_portifolio, n = 1) - 1,
    var_anual = valor_portifolio/lag(valor_portifolio, n = 12) - 1,
    date = ymd(date)
  )

valor_portifolio %>%
  drop_na() %>%
  ggplot(aes(x = date, y = valor_portifolio)) +
  geom_line()


# Índices --------------------------------------------------------

t1 <- empresas_ce %>%
  select(-peso) %>%
  pivot_wider(
    names_from = ticker,
    values_from = price.close
  ) %>%
  mutate_if(is.numeric, ~ifelse(is.na(.), 0, .)) %>%
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
    values_from = peso
  ) %>%
  mutate_if(is.numeric, ~ifelse(is.na(.), 0, .)) %>%
  pivot_longer(
    cols = !date,
    names_to = "ticker",
    values_to = "peso"
  )


t <- t1  %>%
  left_join(t2, by = c("date", "ticker")) %>%
  mutate(
    cdg_date = as.numeric(as_factor(as.character(date)))
  )


indice_laspeyres <- priceIndex(t,
           pvar = "price.close",
           qvar = "peso",
           pervar = "cdg_date",
           prodID = "ticker",
           indexMethod = "laspeyres",
           output = "chained")


data_indice <- data_indice[data_indice >= inicial.date_indice]

indice_df <- tibble(
  date = data_indice,
  indice_laspeyres = as.numeric(indice_laspeyres)*100
)

indice_df <- indice_df %>%
  mutate(
    var_mensal_laspeyres = (indice_laspeyres/lag(indice_laspeyres) - 1),
    var_anual_laspeyres = (indice_laspeyres/lag(indice_laspeyres, 12) - 1)
  )


# Gráficos Índices --------------------------------------------------------


last_row_laspeyres <- indice_df %>%
  tail(1) %>%
  select(date, indice_laspeyres)


g1 <- indice_df %>%
  select(date, indice_laspeyres) %>%
  ggplot(aes(x = date)) +
  geom_line(aes(y = indice_laspeyres), size = 0.5, alpha = 0.8) +
  theme_test() +
  scale_x_date(date_breaks = "1 years", date_labels = "%Y") +
  scale_color_manual(
    labels = c(
      "Índice Laspeyres",
      "Índice Paasche",
      "Índice Fisher"
      ), values = c("red", "blue", "green")
  ) +
  labs(
    title = "Índice das Empresas Cearenses Listadas em Bolsa de Valores",
    subtitle = paste0("Último valor: ", round(last_row_laspeyres$indice_laspeyres, digits = 2)),
    x = "", y = "", col = "",
    caption = "Fonte: Yahoo finance. Elaboração: NUPE/UNIFOR."
  ) +
  theme(
    plot.caption = element_text(hjust = 0)
  )

g1


last_row_laspeyres_var <- indice_df %>%
  tail(1) %>%
  select(date, var_mensal_laspeyres)

g2 <- indice_df %>%
  drop_na() %>%
  select(date, var_mensal_laspeyres) %>%
  ggplot(aes(x = date)) +
  geom_line(aes(y = var_mensal_laspeyres), size = 0.7, alpha = 0.8) +
  theme_test() +
  scale_x_date(date_breaks = "1 years", date_labels = "%Y") +
  scale_color_manual(
    labels = c("Índice Laspeyres", "Índice Fisher"), values = c("red", "blue")
  ) +
  labs(
    title = "Índice das Empresas Cearenses Listadas em Bolsa de Valores",
    subtitle = paste(
      "Retorno diário da carteira (%).",
      "Último valor: ", percent(last_row_laspeyres_var$var_mensal_laspeyres, accuracy = 0.01, decimal.mark = ",")
      ),
    x = "", y = "", col = "",
    caption = "Fonte: Yahoo finance. Elaboração: NUPE/UNIFOR."
  ) +
  theme(
    plot.caption = element_text(hjust = 0)
  )

g2

# IbovespaXIndice ---------------------------------------------------------

ibovespa <- BatchGetSymbols(
  '^BVSP',
  first.date = inicial.date_indice,
  last.date = final.date,
  thresh.bad.data = tresh_bad_data,
  type.return = retorno,
  cache.folder = cache_folder,
  freq.data = periodicidade
)

ibovespa_df <- ibovespa$df.tickers

ibovespa_df <- ibovespa_df %>%
  select(
    ref.date,
    price.close
  ) %>%
  rename(
    ibovespa = price.close,
    date = ref.date
  ) %>%
  mutate(
    var_mensal_ibovespa = (ibovespa/lag(ibovespa) - 1),
    var_anual_ibovespa = (ibovespa/lag(ibovespa, 12) - 1)
  )

indice_df <- indice_df %>%
  left_join(ibovespa_df, by = "date")

# Save data ---------------------------------------------------------------

write_xlsx(
  list(
    "indices" = indice_df,
    "valor_portifolio" = valor_portifolio,
    "base_price.close" = cotacao %>% ggplot2::fortify(),
    "cambio_PTXA" = cambio,
    "price.closeXpesos" = empresas_ce,
    "pesos" = pesos
  ),
  "out/indice_nupe.xlsx"
  )

ggsave(
  filename = "g1.png",
  plot = g1,
  device = "png",
  path = "figs",
  #dpi = 300,
  width = 40,
  height = 15,
  units = "cm"
)

ggsave(
  filename = "g2.png",
  plot = g2,
  device = "png",
  path = "figs",
  dpi = 300,
  width = 15,
  height = 7
)


