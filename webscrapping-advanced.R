library(RCurl)

#myheader <- c(
 # "User-Agent"="Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2272.89 Safari/537.36",
  #"Accept"="text/css,*/*;q=0.1",
  #"Accept-Language"="zh-CN,zh;q=0.8",
  #"Connection"="keep-alive"
  #)

# ----------------- 提取所有页面 -----------------

urllist=0
page=1:80
urllist[page]=paste("http://www.microbell.com/superareport.asp?dockey=%25&dockey2=&doctime1=2015-01-01&doctime2=2015-03-01&report=1&doctype=DocTitle&paget=",page,"&from=",sep='')


# ----------------- 提取所有字段 -----------------
title.all <- NULL
nature.all <- NULL
author.all <- NULL
rate.all <- NULL
for(url in urllist)
{
  temp <- getURL(url,.encoding="gb2312")
  temp2 <- strsplit(temp,"\r\n")[[1]] #以换行符分割
  titadr <- temp2[grep("td width", temp2)+1] # 提取title的行
  natadr <- temp2[grep("td width", temp2)+2] # 提取nature的行
  autadr <- temp2[grep("td width", temp2)+4] # 提取author的行
  ratadr <- temp2[grep("td width", temp2)+6] # 提取rate的行
  
  # ----------------- 提取大标题的字段 -----------------
  
  title <- gregexpr("title.*\\d+..>",titadr)
  title1 <- NULL
  for(i in 1:length(titadr))
  {
    t <- title[[i]]
    title1[i] <- substring(titadr[i], t+7, t+attr(t,"match.length")-3)
  }
  
  # ----------------- 提取属性的字段 -----------------
  
  nature1 <- NULL
  nature <- gregexpr(">.*<",natadr)
  for(i in 1:length(natadr))
  {
    n <- nature[[i]]
    nature1[i] <- substring(natadr[i], n+1, n+attr(n,"match.length")-2)
  }
  
  # ----------------- 提取作者的字段 -----------------
  
  author1 <- NULL
  author <- gregexpr("title.*[\u4e00-\u9fa5]..>",autadr)
  i=1
  for(i in 1:length(autadr))
  {
    a <- author[[i]]
    author1[i] <- substring(autadr[i], a+7, a+attr(a,"match.length")-3)
  }
  
  # ----------------- 提取评级的字段 -----------------
  
  rate <- gregexpr(">.*<",ratadr)
  #也可以这么写rate <- gregexpr(">[\u4e00-\u9fa5]+.*<",ratadr)
  rate1 <- NULL
  for(i in 1:length(ratadr))
  {
    r <- rate[[i]]
    rate1[i] <- substring(ratadr[i], r+1, r+attr(r,"match.length")-2)
  }
  
  title.all <- c(title.all,title1)
  nature.all <- c(nature.all,nature1)
  author.all <- c(author.all,author1)
  rate.all <- c(rate.all,rate1)
  
}

# ------------- 把大标题以“-”符号分割开 ----------
F <- strsplit(title.all,"-")
F_CODE <- sapply(F, function(F) F[3])
F_NAME <- sapply(F, function(F) F[2])
F_DATE <- paste("20", sapply(F, function(F) F[5]),sep='')

F_0001 <- sapply(F, function(F) F[1])
F_0002 <- sapply(F, function(F) F[4])


F_0003 <- author.all
F_0004 <- nature.all
F_0005 <- rate.all

company<-data.frame(F_CODE,F_NAME,F_DATE,F_0001,F_0002,F_0003,F_0004,F_0005)
company

library(RODBC)
odbcDataSources()
conn <- odbcConnect("TXDB",uid="haquser01",pwd="123456",case="tolower",believeNRows=FALSE)
#sqlSave(conn,company,tablename="company",rownames="id")这种方法存不进数据库


for (m in 1:nrow(company)){
  str <- "insert into COMPANY(F_CODE,F_NAME,F_DATE,F_0001,F_0002,F_0003,F_0004,F_0005) VALUES("
  for (n in 1:7){
    str <- paste(str, "'", company[m,n], "'", ",", sep='')
  }
  str <- paste(str,"'", company[m,8], "'", ")", sep='')
  sqlQuery(conn, str)
}


test <- sqlQuery(conn, "select * from COMPANY")

