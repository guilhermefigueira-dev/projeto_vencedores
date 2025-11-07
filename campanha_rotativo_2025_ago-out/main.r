# Pacotes
pacotes <- c('dplyr'
            ,'magrittr'
            ,'ggplot2'
            ,'utils'
            ,'tableone'
            ,'lubridate'
            ,'data.table'
            ,'tidyr'
            ,'ivreg'
            ,'sandwich'
            )
for (p in pacotes) {
    if(requireNamespace(p) == FALSE) install.packages(p)
    invisible(lapply(p, require, character.only = TRUE))
}
rm(list = ls())
invisible(gc())

# Dataframe
df <- read.csv('painel.csv'
              ,colClasses = c('character'
                             ,'character'
                             ,'numeric'
                             ,'numeric'
                             ,'character'
                             ,'character'
                             ,'numeric'
                             ,'numeric'
                             ,'numeric'
                             ,'numeric'
                             )
              ,header = FALSE
              )
colnames(df) <- c('customerdocument'
                 ,'referencedate'
                 ,'tratamento'
                 ,'rating'
                 ,'tpvreference'
                 ,'doctype'
                 ,'survivor'
                 ,'interestrate'
                 ,'saque'
                 ,'amount'
                 )
df <- df %>%
    group_by(customerdocument
            ,referencedate
            ,tratamento
            ,rating
            ,tpvreference
            ,doctype
            ,survivor
            ,interestrate
            ,saque
            ) %>%
    summarise(amount = sum(amount)
             ,ever = max(saque)
             ) %>%
    as.data.table()
df[, referencedate := as.IDate(referencedate)]
setorder(df,customerdocument,referencedate)
df[, aceite := cummax(saque), by = customerdocument]
df[, montante := cumsum(amount), by = customerdocument]

# Graficos
# Conversao fisica
remap1 <- df %>%
    mutate(grupo = if_else(tratamento == 1, 'Trat.', 'Ctrl.')) %>%
    group_by(referencedate,grupo) %>%
    summarise(y = mean(aceite)) %>%
    ungroup()

p1 <- ggplot(remap1) +
    geom_line(aes(x = referencedate, y = y, linetype = grupo)) + 
    theme_bw() +
    labs(title = 'Conversão física', x = '', y = '') +
    scale_y_continuous(name = '', labels = scales::percent) + 
    scale_linetype_manual(values = c('Trat.' = 'solid', 'Ctrl.' = 'dashed')) +
    theme(legend.title = element_blank()
         ,legend.spacing.y = unit(0, 'mm')
         ,legend.position = c(.925, .125)
         ,legend.box.background = element_rect(color = 'black', size = .75)
         )
ggsave('conversaofisica.png', plot = p1, width = 2000, height = 1100, units = 'px')

# Conversao financeira
everT <- df %>%
    filter(tratamento == 1 & referencedate == '2025-08-04') %>%
    summarise(sum(ever)) %>%
    as.numeric()
everC <- df %>%
    filter(tratamento == 0 & referencedate == '2025-08-04') %>%
    summarise(sum(ever)) %>%
    as.numeric()
remap2 <- df %>%
    mutate(N = ifelse(tratamento == 1, everT, everC)
          ,grupo = ifelse(tratamento == 1, 'Trat.', 'Ctrl.')
          ) %>%
    group_by(referencedate,grupo) %>%
    summarise(y = sum(montante/N)) %>%
    ungroup()
p2 <- ggplot(remap2) + 
    geom_line(aes(x = referencedate, y = y, linetype = grupo)) +
    theme_bw() +
    labs(title = 'Conversão financeira normalizada', x = '', y = '') +
    scale_y_continuous(name = '', labels = scales::comma) +
    scale_linetype_manual(values = c('Trat.' = 'solid', 'Ctrl.' = 'dashed')) +
    theme(legend.title = element_blank()
         ,legend.spacing.y = unit(0, 'mm')
         ,legend.position = c(.925, .125)
         ,legend.box.background = element_rect(color = 'black', size = .75)
         )
ggsave('conversaofinanceira.png', plot = p2, width = 2000, height = 1100, units = 'px')

# Teste balanceamento
CreateTableOne(
    vars = c('rating','tpvreference','doctype','survivor'),
    strata = 'tratamento',
    data = df %>% filter(referencedate == '2025-08-04'),
    factorVars = c('tpvreference','doctype','survivor'),
    addOverall = TRUE,
    test = TRUE,
    smd = TRUE
) %>% 
    print(showAllLevels = TRUE,
         smd = TRUE,
         test = TRUE,
         printToggle = TRUE)

# Estimacao
dftomodel <- df %>%
    filter(referencedate == '2025-10-31') %>%
    select(customerdocument,tratamento,rating,tpvreference,doctype,survivor,interestrate,ever,montante)

# Modelo 1
model1 <- ivreg(ever ~ interestrate | tratamento, data = dftomodel)
b1 <- model1$coefficients['interestrate'] %>% as.numeric()
se1 <- (model1 %>% vcovHC(type = 'HC1') %>% diag() %>% sqrt())['interestrate'] %>% as.numeric()
ci901 <- b1 + qnorm(c(0.050, 0.950)) * se1
ci951 <- b1 + qnorm(c(0.025, 0.975)) * se1

print(paste0('beta = ',      round(b1,5)))
print(paste0('se = ',        round(se1,5)))
print(paste0('90%CI-rob = [',round(ci901[1],5),', ',round(ci901[2],5),']'))
print(paste0('95%CI-rob = [',round(ci951[1],5),', ',round(ci951[2],5),']'))

# Modelo 2
model2 <- ivreg(ever ~ interestrate + rating + tpvreference + doctype | tratamento + rating + tpvreference + doctype, data = dftomodel)
b2 <- model2$coefficients['interestrate'] %>% as.numeric()
se2 <- (model2 %>% vcovHC(type = 'HC1') %>% diag() %>% sqrt())['interestrate'] %>% as.numeric()
ci902 <- b2 + qnorm(c(0.050, 0.950)) * se2
ci952 <- b2 + qnorm(c(0.025, 0.975)) * se2

print(paste0('beta = ',      round(b2,5)))
print(paste0('se = ',        round(se2,5)))
print(paste0('90%CI-rob = [',round(ci902[1],5),', ',round(ci902[2],5),']'))
print(paste0('95%CI-rob = [',round(ci952[1],5),', ',round(ci952[2],5),']'))

# Uplift
iC <- mean((dftomodel %>% filter(tratamento == 0))$interestrate)
iT <- mean((dftomodel %>% filter(tratamento == 1))$interestrate)
yC <- mean((dftomodel %>% filter(tratamento == 0))$interestrate)
yT <- yC + b1 * (iT - iC)
lb <- yC + (iT - iC) * (b1 + se1 * qnorm(c(0.025, 0.975)))[2]
ub <- yC + (iT - iC) * (b1 + se1 * qnorm(c(0.025, 0.975)))[1]

upl_pt <- (1 - 0.0465) * (((1 + iT)^(350 / 30) / (1.162))^(9 / 12) - 1) * yT / ((1 - 0.0465) * (((1 + iC)^(350 / 30) / (1.162))^(9 / 12) - 1) * yC) - 1
upl_lb <- (1 - 0.0465) * (((1 + iT)^(350 / 30) / (1.162))^(9 / 12) - 1) * lb / ((1 - 0.0465) * (((1 + iC)^(350 / 30) / (1.162))^(9 / 12) - 1) * yC) - 1
upl_ub <- (1 - 0.0465) * (((1 + iT)^(350 / 30) / (1.162))^(9 / 12) - 1) * ub / ((1 - 0.0465) * (((1 + iC)^(350 / 30) / (1.162))^(9 / 12) - 1) * yC) - 1

print(paste0('Uplift estimado = ', 100 * round(upl_pt,3), '%'))
print(paste0('95%IC-rob = [', 100 * round(upl_lb,3), '%, ' , 100 * round(upl_ub,3), '%]'))
