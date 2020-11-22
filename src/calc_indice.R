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
inicial.date_indice <- ymd('2011-12-29')
final.date <- today()
tresh_bad_data <- 0.001
cache_folder <- 'data/calc_indice'
retorno <- "log"
periodicidade <- "daily"

std_peso = 0.05 # Peso padrão para o cálculo dos índices
nome_indice <- "Índice Laspeyres" # Nome do índice

international_ticker.ce <- c('ARCE')

tickers.ce <- c(
  "PGMN3.SA",
  "BNBR3.SA",
  'COCE5.SA',
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

# Criando DataFrame

empresas_df <- empresas.ce$df.tickers

empresas_df <- empresas_df %>%
  as_tibble() %>%
  select(ticker, ref.date, volume, price.close)  %>%
  drop_na(price.close) %>%
  rename(date = ref.date) %>%
  arrange(date)


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

# Dados mensais

# cotacao <- quantmod::getSymbols(
#   c(tickers.ce, international_ticker.ce),
#   src = 'yahoo',
#   from = inicial.date,
#   to = final.date,
#   periodicity = "daily",
#   auto.assign = T,
#   warnings = F
# ) %>%
#   map(~Cl(get(.))) %>%
#   reduce(merge) %>%
#   `colnames<-`(c(tickers.ce, international_ticker.ce))

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


# Dataframe - empresas Cearenses ------------------------------------------

empresas_df <- empresas_df %>%
  bind_rows(empresas_internacionais_ce_df) %>%
  arrange(date) %>%
  drop_na(price.close) %>%
  bind_rows(ibovespa_df)


# Retirando dados faltantes e datas comemorativas

empresas_df_drop_feriados <- empresas_df %>%
  select(-volume) %>%
  pivot_wider(
    names_from = ticker,
    values_from = price.close,
    values_fn = sum
  ) %>%
  drop_na(Ibovespa) %>%
  arrange(date)


data_indice <- empresas_df_drop_feriados$date

empresas_df <- empresas_df_drop_feriados %>%
  mutate_if(is.numeric, ~ifelse(is.na(.), 0, .)) %>%
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

# Remover posteriormente

# empresas_df <- empresas_df_drop_feriados %>%
#   pivot_longer(
#     !date,
#     names_to = "ticker",
#     values_to = "price.close"
#   ) %>%
#   left_join(
#     empresas_df %>% select(-price.close),
#     by = c("date", 'ticker')
#     ) %>%
#   mutate_if(is.numeric, ~ifelse(is.na(.), 0, .))

# Cálculo dos pesos -------------------------------------------------------

volumes <- empresas_df %>%
  filter(ticker != "Ibovespa") %>%
  select(-price.close) %>%
  mutate(
    date = ymd(
      paste0(str_sub(date, end = 7), "-01")
    )
  ) %>%
  group_by(ticker, date) %>%
  summarise(volume = sum(volume)) %>%
  ungroup() %>%
  pivot_wider(
    names_from = ticker,
    values_from = volume
  ) %>%
  arrange(date)

volumes_12_meses <- volumes %>%
  summarise_if(
    is.numeric,
    ~rollsum(x = ., k = 12, na.pad = T, align = "right")
  )

volumes_12_meses_df <- volumes %>%
  select(date) %>%
  mutate(
    mes = lubridate::month(date)
  ) %>%
  bind_cols(volumes_12_meses) %>%
  # filter(mes %in% c(3, 6, 9, 12)) %>%
  drop_na() %>%
  select(-mes)

pesos_variaveis <- volumes_12_meses_df %>%
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

pesos_variaveis <- empresas_df %>%
  filter(ticker != "Ibovespa") %>%
  mutate(
    ano = year(date),
    mes = month(date)
  ) %>%
  relocate(c(ano, mes), .after = date) %>%
  left_join(pesos_variaveis, by = c("ano", 'mes', 'ticker')) %>%
  select(date, ticker, peso) %>%
  pivot_wider(
    names_from = ticker,
    values_from = peso
  ) %>%
  fill(where(is.numeric), .direction = "downup")

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
  filter(ticker != "Ibovespa") %>%
  left_join(pesos_variaveis, by = c("date", "ticker")) %>%
  select(-volume) %>%
  filter(date >= inicial.date_indice)

# Calculando valor do portifólio ------------------------------------------

# Transformando base em matriz

empresas_matrix <- empresas_df %>%
  filter(ticker != "Ibovespa") %>%
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

# Índices --------------------------------------------------------

t1 <- empresas_ce %>%
  select(-peso) %>%
  pivot_wider(
    names_from = ticker,
    values_from = price.close
  ) %>%
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
  left_join(t2, by = c("date", "ticker"))

t <- t %>%
  mutate(
    peso = ifelse(price.close == 0 & peso == 0.05, NA, peso)
  )

remover_data <- t %>%
  filter(is.na(peso)) %>%
  select(date) %>%
  unique.data.frame()

remover_data <- remover_data$date

t <- t %>%
  filter(!date %in% remover_data) %>%
  mutate(
    cdg_date = as.numeric(as_factor(as.character(date)))
  )



data_indice <- t %>%
  select(-peso, -cdg_date) %>%
  pivot_wider(
    names_from = ticker,
    values_from = price.close
  ) %>% select(date)


indice_laspeyres <- priceIndex(t,
           pvar = "price.close",
           qvar = "peso",
           pervar = "cdg_date",
           prodID = "ticker",
           indexMethod = "laspeyres",
           output = "chained")


# data_indice <- data_indice[data_indice >= inicial.date_indice]

indice_df <- tibble(
  date = data_indice$date,
  indice_laspeyres = as.numeric(indice_laspeyres)*100
)

# IbovespaXIndice ---------------------------------------------------------

# Base de Dados diária

indice_df <- indice_df %>%
  left_join(ibovespa_df, by = "date") %>%
  select(-volume, -ticker) %>%
  rename(ibovespa = price.close) %>%
  mutate(
    var_diaria_laspeyres = (indice_laspeyres/lag(indice_laspeyres) - 1),
    var_52_semanas_laspeyres = (indice_laspeyres/lag(indice_laspeyres, 52) - 1),
    ibovespa = (ibovespa/dplyr::first(ibovespa))*100,
    var_diaria_ibovespa = (ibovespa/lag(ibovespa) - 1),
    var_52_semanas_ibovespa = (ibovespa/lag(ibovespa, 52) - 1)
  )


# Base de dados Mensal

indice_mensal_df <- indice_df %>%
  select(-var_diaria_laspeyres, -var_diaria_ibovespa) %>%
  group_by(month = month(date), year = year(date)) %>%
  slice(which.max(day(date))) %>%
  ungroup() %>%
  select(-month, -year) %>%
  arrange(date) %>%
  mutate(
    date = ymd(paste0(str_sub(date, end = 7), "01")),
    var_mensal_laspeyres = (indice_laspeyres/lag(indice_laspeyres) - 1),
    acum_12_laspeyres = (indice_laspeyres/lag(indice_laspeyres, 12) - 1),
    ibovespa = (ibovespa/dplyr::first(ibovespa))*100,
    var_mensal_ibovespa = (ibovespa/lag(ibovespa) - 1),
    acum_12_ibovespa = (ibovespa/lag(ibovespa, 12) - 1)
  )

# Gráficos diários --------------------------------------------------------

# ìndice diário

last_row_laspeyres <- indice_df %>%
  tail(1) %>%
  select(date, indice_laspeyres)


last_row_ibovespa <- indice_df %>%
  tail(1) %>%
  select(date, ibovespa)

g1 <- indice_df %>%
  select(date, indice_laspeyres, ibovespa) %>%
  pivot_longer(!date) %>%
  mutate(
    name = factor(
      name,
      levels = c("indice_laspeyres", "ibovespa")
      ),
    name = fct_recode(
      name,
      "Ibovespa" = "ibovespa",
      "Índice Laspeyres" = "indice_laspeyres"
      )
    ) %>%
  ggplot(aes(x = date, y = value, col = name)) +
  geom_line(size = 0.7, alpha = 0.8) +
  theme_test() +
  scale_x_date(date_breaks = "1 years", date_labels = "%Y") +
  labs(
    title = "Índice Diário das Empresas Cearenses Listadas em Bolsa de Valores.",
    subtitle = paste0(
      "Último valor Índice Laspeyres: ",
      round(last_row_laspeyres$indice_laspeyres, digits = 2),
      " / Último valor Ibovespa: ",
      round(last_row_ibovespa$ibovespa, digits = 2),
      " / Data de referência: ", format(last_row_laspeyres$date, "%d de %B de %Y.")
      ),
    x = "", y = "", col = "",
    caption = "Fonte: Yahoo finance. Elaboração: NUPE/UNIFOR."
  ) +
  theme(
    plot.caption = element_text(hjust = 0),
    legend.position = c(0.1, 0.7),
    legend.text = element_text(size = 12),
    legend.key = element_rect(fill = "white", colour = "black"),
    legend.title = element_text(face = "bold"),
    legend.key.width =  unit(1.5, "lines")
  )

g1

# Variação diária

last_row_laspeyres_var <- indice_df %>%
  tail(1) %>%
  select(date, var_diaria_laspeyres)

last_row_ibovespa_var <- indice_df %>%
  tail(1) %>%
  select(date, var_diaria_ibovespa)

g2 <- indice_df %>%
  drop_na() %>%
  select(date, var_diaria_laspeyres, var_diaria_ibovespa) %>%
  pivot_longer(!date) %>%
  mutate(
    name = factor(
      name,
      levels = c("var_diaria_laspeyres", "var_diaria_ibovespa")
    ),
    name = fct_recode(
      name,
      "Ibovespa" = "var_diaria_ibovespa",
      "Índice Laspeyres" = "var_diaria_laspeyres"
    )
  ) %>%
  ggplot(aes(x = date, y = value, col = name)) +
  geom_line(size = 0.7, alpha = 0.8) +
  geom_hline(yintercept = 0) +
  scale_y_continuous(labels = percent_format(0.1, decimal.mark = ",", big.mark = ".")) +
  facet_wrap(~name, nrow = 2, scales = "free") +
  theme_test() +
  scale_x_date(date_breaks = "1 years", date_labels = "%Y") +
  labs(
    title = "Variação diária - Empresas Cearenses.",
    subtitle = paste0(
      "Último valor Índice Laspeyres: ",
      percent(last_row_laspeyres_var$var_diaria_laspeyres, 0.01, decimal.mark = ",", big.mark = "."),
      " / Último valor Ibovespa: ",
      percent(last_row_ibovespa_var$var_diaria_ibovespa, 0.01, decimal.mark = ",", big.mark = "."),
      " / Data de referência: ", format(last_row_laspeyres_var$date, "%d de %B de %Y.")
    ),
    x = "", y = "", col = "",
    caption = "Fonte: Yahoo finance. Elaboração: NUPE/UNIFOR."
  ) +
  theme(
    plot.caption = element_text(hjust = 0),
    legend.position = "none"
  )

g2

# Variação 52 semanas

last_row_laspeyres_var_52 <- indice_df %>%
  tail(1) %>%
  select(date, var_52_semanas_laspeyres)

last_row_ibovespa_var_52 <- indice_df %>%
  tail(1) %>%
  select(date, var_52_semanas_ibovespa)

g3 <- indice_df %>%
  drop_na() %>%
  select(date, var_52_semanas_laspeyres, var_52_semanas_ibovespa) %>%
  pivot_longer(!date) %>%
  mutate(
    name = factor(
      name,
      levels = c("var_52_semanas_laspeyres", "var_52_semanas_ibovespa")
    ),
    name = fct_recode(
      name,
      "Ibovespa" = "var_52_semanas_ibovespa",
      "Índice Laspeyres" = "var_52_semanas_laspeyres"
    )
  ) %>%
  ggplot(aes(x = date, y = value, col = name)) +
  geom_line(size = 0.7, alpha = 0.8) +
  geom_hline(yintercept = 0) +
  scale_y_continuous(labels = percent_format(0.1, decimal.mark = ",", big.mark = ".")) +
  facet_wrap(~name, nrow = 2, scales = "free") +
  theme_test() +
  scale_x_date(date_breaks = "1 years", date_labels = "%Y") +
  labs(
    title = "Variação (52 semanas) - Empresas Cearenses.",
    subtitle = paste0(
      "Último valor Índice Laspeyres: ",
      percent(last_row_laspeyres_var_52$var_52_semanas_laspeyres, 0.01, decimal.mark = ",", big.mark = "."),
      " / Último valor Ibovespa: ",
      percent(last_row_ibovespa_var_52$var_52_semanas_ibovespa, 0.01, decimal.mark = ",", big.mark = "."),
      " / Data de referência: ", format(last_row_laspeyres_var_52$date, "%d de %B de %Y.")
    ),
    x = "", y = "", col = "",
    caption = "Fonte: Yahoo finance. Elaboração: NUPE/UNIFOR."
  ) +
  theme(
    plot.caption = element_text(hjust = 0),
    legend.position = "none"
  )

g3

# Gráficos mensais --------------------------------------------------------

# ìndice Mensal

g4 <- indice_mensal_df %>%
  select(date, indice_laspeyres, ibovespa) %>%
  pivot_longer(!date) %>%
  mutate(
    name = factor(
      name,
      levels = c("indice_laspeyres", "ibovespa")
    ),
    name = fct_recode(
      name,
      "Ibovespa" = "ibovespa",
      "Índice Laspeyres" = "indice_laspeyres"
    )
  ) %>%
  ggplot(aes(x = date, y = value, col = name)) +
  geom_line(size = 0.8, alpha = 0.8) +
  theme_test() +
  scale_x_date(date_breaks = "1 years", date_labels = "%Y") +
  labs(
    title = "Índice Mensal das Empresas Cearenses Listadas em Bolsa de Valores.",
    subtitle = paste0(
      "Último valor Índice Laspeyres: ",
      round(last_row_laspeyres$indice_laspeyres, digits = 2),
      " / Último valor Ibovespa: ",
      round(last_row_ibovespa$ibovespa, digits = 2),
      " / Mês de referência: ", str_to_sentence(format(last_row_laspeyres$date, "%B de %Y."))
    ),
    x = "", y = "", col = "",
    caption = "Fonte: Yahoo finance. Elaboração: NUPE/UNIFOR."
  ) +
  theme(
    plot.caption = element_text(hjust = 0),
    legend.position = c(0.1, 0.7),
    legend.text = element_text(size = 12),
    legend.key = element_rect(fill = "white", colour = "black"),
    legend.title = element_text(face = "bold"),
    legend.key.width =  unit(1.5, "lines")
  )

g4


# variação Mensal


last_row_laspeyres_mensal <- indice_mensal_df %>%
  tail(1) %>%
  select(date, var_mensal_laspeyres)


last_row_ibovespa_mensal <- indice_mensal_df %>%
  tail(1) %>%
  select(date, var_mensal_ibovespa)

g5 <- indice_mensal_df %>%
  select(date, var_mensal_laspeyres, var_mensal_ibovespa) %>%
  pivot_longer(!date) %>%
  mutate(
    name = factor(
      name,
      levels = c("var_mensal_laspeyres", "var_mensal_ibovespa")
    ),
    name = fct_recode(
      name,
      "Ibovespa" = "var_mensal_ibovespa",
      "Índice Laspeyres" = "var_mensal_laspeyres"
    )
  ) %>%
  drop_na(value) %>%
  ggplot(aes(x = date, y = value, col = name)) +
  geom_line(size = 0.8, alpha = 0.8) +
  geom_hline(yintercept = 0) +
  theme_test() +
  scale_x_date(date_breaks = "1 years", date_labels = "%Y") +
  scale_y_continuous(labels = percent_format(0.1, decimal.mark = ",", big.mark = ".")) +
  facet_wrap(~name, nrow = 2, scales = "free") +
  labs(
    title = "Variação mensal - Empresas Cearenses.",
    subtitle = paste0(
      "Último valor Índice Laspeyres: ",
      percent(last_row_laspeyres_mensal$var_mensal_laspeyres, 0.01, decimal.mark = ","),
      " / Último valor Ibovespa: ",
      percent(last_row_ibovespa_mensal$var_mensal_ibovespa, 0.01, decimal.mark = ","),
      " / Mês de referência: ", str_to_sentence(format(last_row_laspeyres_mensal$date, "%B de %Y."))
    ),
    x = "", y = "", col = "",
    caption = "Fonte: Yahoo finance. Elaboração: NUPE/UNIFOR."
  ) +
  theme(
    plot.caption = element_text(hjust = 0),
    legend.position = 'none',
    legend.text = element_text(size = 12),
    legend.key = element_rect(fill = "white", colour = "black"),
    legend.title = element_text(face = "bold"),
    legend.key.width =  unit(1.5, "lines")
  )


g5

# Acumulado dos últimos 12 meses

last_row_laspeyres_12_meses <- indice_mensal_df %>%
  tail(1) %>%
  select(date, acum_12_laspeyres)


last_row_ibovespa_12_meses <- indice_mensal_df %>%
  tail(1) %>%
  select(date, acum_12_ibovespa)

g6 <- indice_mensal_df %>%
  select(date, acum_12_laspeyres, acum_12_ibovespa) %>%
  pivot_longer(!date) %>%
  mutate(
    name = factor(
      name,
      levels = c("acum_12_laspeyres", "acum_12_ibovespa")
    ),
    name = fct_recode(
      name,
      "Ibovespa" = "acum_12_ibovespa",
      "Índice Laspeyres" = "acum_12_laspeyres"
    )
  ) %>%
  drop_na(value) %>%
  ggplot(aes(x = date, y = value, col = name)) +
  geom_line(size = 0.8, alpha = 0.8) +
  geom_hline(yintercept = 0) +
  theme_test() +
  scale_x_date(date_breaks = "1 years", date_labels = "%Y") +
  labs(
    title = "Variação acumulada dos útlimos 12 meses - Empresas Cearenses",
    subtitle = paste0(
      "Último valor Índice Laspeyres: ",
      percent(last_row_laspeyres_12_meses$acum_12_laspeyres, 0.01, decimal.mark = ","),
      " / Último valor Ibovespa: ",
      percent(last_row_ibovespa_12_meses$acum_12_ibovespa, 0.01, decimal.mark = ","),
      " / Mês de referência: ", str_to_sentence(format(last_row_laspeyres_12_meses$date, "%B de %Y."))
    ),
    x = "", y = "", col = "",
    caption = "Fonte: Yahoo finance. Elaboração: NUPE/UNIFOR."
  ) +
  theme(
    plot.caption = element_text(hjust = 0),
    legend.position = c(0.1, 0.9),
    legend.text = element_text(size = 12),
    legend.key = element_rect(fill = "white", colour = "black"),
    legend.title = element_text(face = "bold"),
    legend.key.width =  unit(1.5, "lines")
  )

g6

# Save data ---------------------------------------------------------------

# Dadods em formato wide

empresas_df.wide <- empresas_df  %>%
  select(-volume) %>%
  pivot_wider(
    names_from = ticker,
    values_from = price.close
  ) %>%
  arrange(date)

# Dados Mensais

empresas_mensal_df <- empresas_df %>%
  select(-volume) %>%
  pivot_wider(
    names_from = ticker,
    values_from = price.close
  ) %>%
  group_by(month = month(date), year = year(date)) %>%
  slice(which.max(day(date))) %>%
  ungroup() %>%
  select(-month, -year) %>%
  mutate(
    date = ymd(paste0(str_sub(date, end = 7), "01"))
  ) %>%
  arrange(date)


write_xlsx(
  list(
    'indice_mensal' = indice_mensal_df,
    "indice_diario" = indice_df,
    "base_mensal_price.close" = empresas_mensal_df %>% filter(date >= "2012-12-01"),
    "base_diaria_price.close" = empresas_df.wide %>% filter(date >= "2012-12-29"),
    "price.closeXpesos" = empresas_ce %>% filter(peso > 0),
    "cambio_PTXA" = cambio,
    "pesos_atuais" = pesos
  ),
  "out/indice_nupe.xlsx"
  )

ggsave(
  filename = "g1.png",
  plot = g1,
  device = "png",
  path = "figs",
  dpi = 600,
  width = 40,
  height = 15,
  units = "cm"
)

ggsave(
  filename = "g2.png",
  plot = g2,
  device = "png",
  path = "figs",
  dpi = 600,
  width = 15,
  height = 7
)

ggsave(
  filename = "g3.png",
  plot = g3,
  device = "png",
  path = "figs",
  dpi = 600,
  width = 15,
  height = 7
)


ggsave(
  filename = "g4.png",
  plot = g4,
  device = "png",
  path = "figs",
  dpi = 600,
  width = 40,
  height = 15,
  units = "cm"
)


ggsave(
  filename = "g5.png",
  plot = g5,
  device = "png",
  path = "figs",
  dpi = 600,
  width = 15,
  height = 7
)

ggsave(
  filename = "g6.png",
  plot = g6,
  device = "png",
  path = "figs",
  dpi = 600,
  width = 15,
  height = 7
)
