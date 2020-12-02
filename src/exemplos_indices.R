

library(tidyverse)
library(IndexNumR)

# Exemplo

df <- tibble(
  commodity = c('a', 'b', 'c', 'd'),
  p_0 = c(10, 15, 40, 25),
  q_0 = c(5, 4, 2, 3),
  p_1 = c(20, 25, 60, 40),
  q_1 = c(2, 8, 6, 4)
)

df


df %>%
  mutate(
    P_0_Q_0 = p_0 * q_0,
    P_1_Q_0 = p_1 * q_0,
    P_0_Q_1 = p_0 * q_1
  )


df %>%
  pivot_longer(
    cols = commodity
  )




df2 <- tibble(
  commodity = rep(letters[1:3], 5),
  periodo = c(1, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 4, 5, 5, 5),
  pvar = c(12, 5, 20, 15, 10, 25, 18, 20, 35, 24, 30, 45, 30, 60, 50),
  qvar = c(3, 7, 3, 4, 9, 4, 5, 8, 5, 5, 7, 6, 6, 6, 5)
) %>%
  arrange(commodity)


values(df2,
       pvar = "pvar",
       qvar = "qvar",
       pervar = "periodo",
       prodID = "commodity"
)

unitValues(df2,
           pvar = "pvar",
           qvar = "qvar",
           pervar = "periodo",
           prodID = "commodity")





priceIndex(df2,
           pvar = "pvar",
           qvar = "qvar",
           pervar = "periodo",
           prodID = "commodity",
           indexMethod = "laspeyres",
           output = 'fixedBase'
           )

priceIndex(df2,
                pvar = "pvar",
                qvar = "qvar",
                pervar = "periodo",
                prodID = "commodity",
                indexMethod = "laspeyres",
                output = "chained")

priceIndex(df2,
                pvar = "pvar",
                qvar = "qvar",
                pervar = "periodo",
                prodID = "commodity",
                indexMethod = "laspeyres",
                output = "pop")



writexl::write_xlsx(
  list(
    df = df2,
    x = as_tibble(x),
    y = as_tibble(y),
    z = as_tibble(z)
    ),
  "teste.xlsx")













