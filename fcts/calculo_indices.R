
# Funções para cálculo de índices -----------------------------------------

indices_variavel_fluxo <- function(df, date, var) {
  
  colunas <- df %>% 
    select({{var}}) %>% 
    colnames()
  
  df %>%
    mutate(
      var_mensal = {{var}} / dplyr::lag({{var}}) - 1,
      var_intranual = {{var}} / dplyr::lag({{var}}, 12) - 1,
      var_acum_ano = cumsum({{var}})
    ) %>%
    group_by(year({{date}})) %>%
    mutate(
      var_acum_ano = cumsum({{var}})
    ) %>%
    ungroup() %>%
    mutate(
      var_acum_ano = var_acum_ano / dplyr::lag(var_acum_ano, 12) - 1,
      var_acum_12 = runner::sum_run({{var}}, k = 12),
      var_acum_12 = var_acum_12 / dplyr::lag(var_acum_12, 12) - 1,
    ) %>%
    select(!starts_with("year")) %>% 
    relocate(
      var_mensal:var_acum_12, .after = {{var}}
    ) %>% 
    select(
      {{date}}, 
      {{var}},
      var_mensal, 
      var_intranual, 
      var_acum_ano, 
      var_acum_12
      ) %>% 
    rename_at( 
      vars(contains("var")), 
      list(~paste0(., "_", colunas))
    )
}
