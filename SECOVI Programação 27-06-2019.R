setwd("C:\\Users\\edfmc\\Desktop\\@UnB\\ESTAT\\Projetos\\projeto 18 - secovi")


cidade1=1
cidade2=53

library(parallel)

# Calculate the number of cores
no_cores <- detectCores() - 1

# Initiate cluster
cl <- makeCluster(no_cores)


## M?scara para entrada dos dados (nomear as variav?is)
dados <- data.frame(matrix(rep(0,12),nrow=1))
names(dados) <- c("cid","tip2","int","bai","qua","ban","valor","com","preco","end","area2", "cod")


## Endere?os fixos
comeco <- 'http://www.wimoveis.com.br/'
final <- '-df.html'


## Partes do endere?o que mudam
cidades <- c('aguas-claras', 'brazlandia', 'candangolandia', 'ceilandia','cruzeiro','gama',
             'guara', 'nucleo-bandeirante', 'paranoa', 'planaltina', 'recanto-das-emas',
             'riacho-fundo', 'samambaia', 'santa-maria', 'sao-sebastiao', 'setor-industrial',
             'sobradinho', 'taguatinga', 'varjao', 'vicente-pires', 'vila-estrutural')

bb <- c("Asa Norte", "Asa Sul", "Lago Sul", "Setor Hab Jardim Botanico", "Noroeste", "Centro",
        "Granja Do Torto", "Jardim Botanico", "Lago Norte", "Octogonal", "Park Sul", "Park Way",
        "Ponte Alta de Cima", "Setor Comercial Norte", "Setor Comercial Sul",
        "Setor De Clubes Esportivos Sul", "Setor De Grandes Areas Sul",
        "Setor de Habitac-es Individuais Norte", "Setor De Industria Graficas", 
        "Setor De Industrias", "Setor Habitacional Jardim Botanico", "Setor Habitacional Tororo",
        "Setor Hoteleiro Norte", "Setor Militar Urbano", "Setores Complementares", "Sudoeste",
        "Taquari", "Vila Da Telebrasilia", "Vila Planalto", "Zona Civico-administrativa", 
        "Zona Industrial", "Zona Rural")

bairros <- c(cidades, bb)
bairros.bsb <- gsub(" ", "-", tolower(bb))
brasilia <- paste(bairros.bsb, "brasilia", sep = "-")
c1 <- cbind(1:53, c(cidades, brasilia))

tipo.ap <- tolower(c("Padrao", "Kitnet-Studio", "Flat", "Cobertura", "Duplex", "Loft"))
ap <- paste("apartamentos", tipo.ap, sep = "-")

tipo.casa <- gsub(" ", "-", tolower(c("Padrao", "Casa de Condominio", "Sobrado", "Casa de Vila")))
casa <- paste("casas", tipo.casa, sep = "-")

tipo.comercial <- gsub(" ", "-", tolower(c("Conjunto Comercial-sala", "Loja-Salao", "Box-Garagem",
                                           "Predio Inteiro", "Consultorio", "Andar", "Clinica",
                                           "Ponto Comercial", "Casa Comercial", "area Comercial",
                                           "Galpao-Deposito-Barracao", "Hotel",
                                           "Loja de Shopping-Centro Comercial", "Pousada-Chale",
                                           "Area Industrial")))
comercial <- paste("comerciais", tipo.comercial, sep = "-")

tipo.terreno <- tolower(c("Loteamento-Condominio", "Terreno-Padrao"))
terreno <- paste("terrenos", tipo.terreno, sep = "-")

tipo.rural <- c("chacara", "fazenda")
rural <- paste("rurais", tipo.rural, sep = "-")

tipos <- c(ap, casa, comercial, terreno, rural)
c2 <- cbind(1:length(tipos), tipos)
vv <- c('aluguel', 'venda')


## Extra??o dos dados para todos os endere?os de interesse
erro404 <- c("")
erronp <- c("")
erropreco <- c("")

pb <- winProgressBar(title = "Coleta de Dados SECOVI", label = "0%", min = 0, max = 100, initial = 0)
pbcontagem <- 0
pbtotal <- 2*nrow(c1)*nrow(c2)

for(a in cidade1:cidade2) {
  n1 = nrow(dados)
  for(b in 1:nrow(c2)) {
    for(q in 1:2) {
      pbcontagem <- pbcontagem + 1

      endereco <- paste(comeco, c2[b, 2], "-", vv[q], "-", c1[a, 2], final, sep = "")
      for(i in 1:5){
        teste <- tryCatch(readLines(endereco, n = 1),error=invisible)
        if(!grepl("cannot open the connection",as.character(teste))) {break} 
        else {print(paste("retry",i,"não funcionou"))}
      }
      teste <- try(readLines(endereco, n = 1), silent = T)
      if(inherits(teste, "try-error")) {erro404 <- rbind(erro404, endereco)} else {
        for(i in 1:5){
          thepage <- tryCatch(readLines(endereco,encoding = 'UTF-8'),error=invisible)
          if(!grepl("cannot open the connection",as.character(thepage))) {break} 
          else {print(paste("retry",i,"não funcionou"))}
        }
        
        ## Identificar o n?mero de p?ginas para cada endere?o
        #paginas <- regmatches(thepage, regexpr('>[0-9.]+?<\\/strong>', thepage))
        paginas <- regmatches(thepage, regexpr("const totalPosting = '[0-9]+(\\.[0-9]+)?'", thepage))
        #pagina <- as.numeric(gsub(">|<\\/strong>|\\.", "", paginas))
        pagina <- as.numeric(sub("\\.","",sub("'","",gsub("const totalPosting = '", "", paginas))))
        pag <- ceiling(pagina/20) # arredondar sempre pra cima
        if((length(pag) == 0)) { erronp <- rbind(erronp, endereco)
        np = 0} else np = pag
        
        if(np == 0) break
        
        ## Extrair de todas as p?ginas de determinado endere?o
        for(x in 1:np) {
          endereco <- paste(comeco, c2[b, 2], "-", vv[q], "-", c1[a, 2],
                            "-df-pagina-", x, ".html", sep = "")
          
          for(i in 1:5){
            page <- tryCatch(readLines(endereco,encoding = 'UTF-8'),error=invisible)
            if(!grepl("cannot open the connection",as.character(page))) {break} 
            else {print(paste("retry",i,"não funcionou"))}
          }
          
          #partes0 = unlist(strsplit(paste(page, collapse = ""), 'post-titulo'))
          #partes0 = unlist(strsplit(paste(page, collapse = ""), '"aviso-data-price"'))
          partes0 = unlist(strsplit(paste(page, collapse = ""), 'data-price'))
          
          #partes0 <- partes0[-1]
          #partes = partes0[-1]
          
          if(length(partes0) == 0) break
          #compreco <- grepl('price-clasificado', partes0)
          #compreco <- grepl('aviso-data-price-value', partes0)
          compreco <- grepl('class="first-price"', partes0)
          partes <- partes0[compreco == T]
          n <- length(partes)
          
          #Banheiro
          #vbanho <- grepl('post-banos', partes)
          #vbanho <- grepl('[0-9]+\t\t\t\t\t\t\t<span>\t\t\t\t\t\t\t\t\tBanheiro', partes)
          vbanho <- grepl('<li><b>[0-9]+ Banheiro(s)?<', partes)
          #banho <- unlist(regmatches(partes, gregexpr('[0-9](\t)"post-banos\">(\t)+[0-9]', partes)))
          #banho <- unlist(regmatches(partes, gregexpr('[0-9]+\t\t\t\t\t\t\t<span>\t\t\t\t\t\t\t\t\tBanheiro', partes)))
          banho <- unlist(regmatches(partes, gregexpr('<li><b>[0-9]+ Banheiro(s)?<', partes)))
          #banho <- as.numeric(sub('"post-banos\">(\t)+', '', banho))
          #banho <- as.numeric(sub('\t\t\t\t\t\t\t<span>\t\t\t\t\t\t\t\t\tBanheiro', '', banho))
          banho <- as.numeric(sub('<li><b>([0-9]+) Banheiro(s)?<', '\\1', banho))
          ban <- rep(NA, n)
          ban[vbanho] <- banho
          
          #?rea
          #varea <- grepl("post-m2totales", partes)
          #varea <- grepl("m&sup2", partes)
          varea <- grepl("[0-9]+ m. área total", partes)
          #area <- unlist(regmatches(partes, gregexpr('"post-m2totales\">(\t)+[0-9]+', partes)))
          #area <- unlist(regmatches(partes, gregexpr('[0-9]+ m&sup2', partes)))
          area <- unlist(regmatches(partes, gregexpr('[0-9]+ m. área total', partes)))
          #area <- as.numeric(sub('"post-m2totales\">(\t)+', '', area))
          #area <- as.numeric(sub(' m&sup2', '', area))
          area <- as.numeric(sub(' m. área total', '', area))
          area2 <- rep(NA, n)
          area2[varea] <- area
          
          #Quartos
          #vquarto <- grepl('post-habitaciones',partes)
          #vquarto <- grepl('[0-9]+\t\t\t\t\t\t\t\t<span>\t\t\t\t\t\t\t\t\t\tQuarto',partes)
          #vquarto <- grepl('[0-9]+\t\t\t\t\t\t\t<span>\t\t\t\t\t\t\t\t\tQuarto', partes)
          vquarto <- grepl('<li><b>[0-9]+ Quarto(s)?<', partes)
          #quarto <- unlist(regmatches(partes, gregexpr('"post-habitaciones\">(\t)+[0-9]+',partes)))
          #quarto <- unlist(regmatches(partes, gregexpr('[0-9]+\t\t\t\t\t\t\t<span>\t\t\t\t\t\t\t\t\tQuarto',partes)))
          quarto <- unlist(regmatches(partes,gregexpr('<li><b>[0-9]+ Quarto(s)?<',partes)))
          #quarto <- as.numeric(sub('"post-habitaciones\">(\t)+', '', quarto))
          #quarto <- as.numeric(sub('\t\t\t\t\t\t\t<span>\t\t\t\t\t\t\t\t\tQuarto', '', quarto))
          quarto <- as.numeric(sub('<li><b>([0-9]+) Quarto(s)?<', '\\1', quarto))
          qua <- rep(NA, n)
          qua[vquarto] <- quarto
          
          #C?digo
          cod0 <- gsub("(propriedades/)|(.html)", "", regmatches(partes, regexpr("propriedades/.*?html", partes)))
          cod = rep(NA,n)
          cod[varea] <- cod0
          
          #Endere?o
          #end0 <- gsub('dl-aviso-link" title\\="|\\">', "", regmatches(partes, regexpr('dl-aviso-link" title\\=".*?\\">', partes)))
          end0 <- gsub('target="_blank">(\\\t)+(.*?)(\\\t)+</a>', "\\2", regmatches(partes, regexpr('target="_blank">.*?</a>', partes)))
          end = rep(NA,n)
          end[varea] <- end0
          
          #Pre?o
          #preco0 <- as.numeric(gsub("[[:punct:]]|[A-z]| ","",regmatches(partes,
          #  regexpr('aviso-data-price-value">(R\\$) [0-9]+(\\.)?[0-9]*(\\.)?[0-9]*</span>', partes))))
          preco0 <- as.numeric(gsub("[[:punct:]]|[A-z]| ","",regmatches(partes,regexpr('=\"(R\\$) [0-9]+(\\.)?[0-9]*(\\.)?[0-9]*"',partes))))
          preco = rep(NA,n)
          if(length(preco0) != 0)preco[varea] <- preco0
          
          valor0 <- preco0/area2[varea]
          valor = rep(NA,n)
          if(length(valor0) != 0)valor[varea] <- valor0
          
          
          if(a > length(cidades)){
            cid <- rep("brasilia", n)
          }else{
            cid <- rep(c1[a, 2], n)
          }
        
          bai <- rep(bairros[a], n)
          tip2 <- rep(c2[b, 2], n)
          int <- rep(vv[q], n)
          
          #1=comercial
          #2=habitacional
          #3=terreno
          #4=rural
          com <- ifelse(b > 10 & b <=25, 1, ifelse(b <= 10, 2, ifelse(b == 26 | b == 27, 3, 4)))
          com <- rep(com, n)
          
          novo <- data.frame(cid, tip2, int, bai, qua, ban, valor, com, preco, end, area2, cod)
          dados <- rbind(dados, novo)
          
          setWinProgressBar(pb,pbcontagem/pbtotal*100,label=sprintf(paste("%.2f%%",'|',c1[a, 2],'|',nrow(dados),sep=''),round(pbcontagem/pbtotal*100,2)))
        }
      }
    }
  }
  print(paste(c1[a, 2],' ',(nrow(dados)-n1),' imóveis',sep=''))
}

stopCluster(cl)

names(dados) <- c("Cidade", "Tipo", "Interesse", "Bairro", "Quartos", "Banheiros", "Valor", "Comercial",
                  "Preco", "Endereco", "Area", "cod")

dados <- dados[-1, ]

time <- Sys.time()
meses <- c("Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho", "Julho",
           "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro")

save(dados, file = paste("Dados ", substr(as.Date(time), 9, 10), "-", substr(as.Date(time), 6, 7),
                         "-", substr(as.Date(time), 1, 4), ".Rdata", sep = ""))

library(foreign)
write.foreign(dados, paste("dados ", substr(as.Date(time), 9, 10), "-", substr(as.Date(time), 6, 7),
                           "-", substr(as.Date(time), 1, 4), "-", nrow(dados), ".txt", sep = ""),
              paste("dados ", substr(as.Date(time), 9, 10), "-", substr(as.Date(time), 6, 7), "-",
                    substr(as.Date(time), 1, 4), "-", nrow(dados), ".sas", sep = ""), package = "SAS")
