# ----------groundworks---------------
rm(list=ls())
library("RCurl")
library("RODBC")
conn <- odbcConnect("TXDB",uid="haquser01",pwd="123456",case="tolower",believeNRows=FALSE)
#若不清楚数据源名称，可使用函数odbcDataSources()

#-------------日志存储函数----------
logRecTxt = function(fileName, log)
{
  # 本函数将日志保存为文本格式
  #   log = '1234'
  #   fileName = 'test.txt'
  
  log = paste(Sys.time(), log, sep=",")
  write.table(log, fileName, quote=F, sep=",",append=T,col.names=F,row.names=F)
}

# ----------------- 提取所有页面 -----------------
getURL_list <- function(dayfirst,daylast)
{
  #本函数用于得到需要爬虫抓取的网页地址
  #
  #输入：
  #dayfirst:研报检索日期的初始值，这里设定为“2015-01-01”
  #daylast:研报检索日期的最终值，这里设定为“2015-03-01”
  
  #
  #输出：
  #urllist:网页完整地址的字符串向量
  
  #得到检索日期区间内的所有页面地址
  page <- 0
  urllist <- NULL
  while(page>=0)
  {
    page <- page+1
    urllist[page]=paste("http://www.microbell.com/superareport.asp?dockey=%25&dockey2=&doctime1=",dayfirst,"&doctime2=",daylast,"&report=1&doctype=DocTitle&paget=",page,"&from=",sep='')
    t1 <- getURL(urllist[page],.encoding="gb2312")
    t2 <- strsplit(t1,"\r\n")[[1]]
    t3 <- t2[grep("td width", t2)]
    if (length(t3)==3){
      break
    }
  }
  urllist <- urllist[1:(page-1)]
  return(urllist)
}

#-----------试开url函数-----------
readURL1 <- function(urllist, N)
{
  # 本函数用于试开目标网页并得到符合要求的字段
  #
  # 输入：
  # urllist: 全部80个网页组成的长度为80的向量
  # N :尝试打开次数，一个整数
  
  #
  # 输出：
  # temps：网页信息代码，长度为80的字符向量
  
  
  # 读取不成功，则反复读取N次。若N次读取不成功则返回NULL，记录错误日志。
  temps <- NULL
  for (url in urllist){
    temp=tryCatch( {getURL(url,.encoding="gb2312")},
                   error=function(err){
                     temp = NULL
                   })
    temps <- c(temps,temp)
    j = 1
    while(is.null(temp))
    {
      if(j>N)
      {
        logRecTxt("test.txt",paste("打不开网页",url))
        break
      }
      temp=tryCatch( {getURL(url,.encoding="gb2312")},
                     error=function(err){
                       temp = NULL
                     })
      temps <- c(temps,temp)
      j = j+1
    } 
  }
  
  return(temps)
}




# ----------------- 提取大标题的字段 -----------------

fetch.title <- function(temps)
{
  #本函数用于从网页信息代码中提取研报的大标题，范例格式为“财富证券-中南传媒-601098-数字教育和互联网IP的隐形冠军-150923”
  #
  #输入：
  #temps:网页信息代码，长度为80的字符向量
  
  #
  #输出：
  #title.all:从2015-01-01到2015-03-01之间公司调研栏目下的所有研报标题，是字符串向量
  title.all <- NULL
  for(temp1 in temps)
  {
    temp2 <- strsplit(temp1,"\r\n")[[1]] #以换行符分割
    titadr <- temp2[grep("td width", temp2)+1] # 提取title的行
    
    title <- gregexpr("title.*\\d+..>",titadr)
    title1 <- NULL
    for(i in 1:length(titadr))
    {
      t <- title[[i]]
      title1[i] <- substring(titadr[i], t+7, t+attr(t,"match.length")-3)
    }
    title.all <- c(title.all,title1)
  }
  
  return(title.all)
}

# ----------------- 提取属性的字段 -----------------

fetch.nature <- function(temps)
{
  #本函数用于从网页信息代码中提取研报的属性，即其所在栏目，这里一律为“公司调研”
  #
  #输入：
  #temps:网页信息代码，长度为80的字符向量
  
  #
  #输出：
  #nature.all:从2015-01-01到2015-03-01之间公司调研栏目下的所有研报标题，是字符串向量
  nature.all <- NULL
  for(temp1 in temps){
    temp2 <- strsplit(temp1,"\r\n")[[1]] #以换行符分割
    natadr <- temp2[grep("td width", temp2)+2] # 提取nature的行
    
    nature1 <- NULL
    nature <- gregexpr(">.*<",natadr)
    for(i in 1:length(natadr))
    {
      n <- nature[[i]]
      nature1[i] <- substring(natadr[i], n+1, n+attr(n,"match.length")-2)
      
    }
    nature.all <- c(nature.all,nature1)
  }
  
  return(nature.all) 
}

# ----------------- 提取作者的字段 -----------------

fetch.author <- function(temps)
{
  #本函数用于从网页信息代码中提取研报作者
  #
  #输入：
  #temps:网页信息代码，长度为80的字符向量
  
  #
  #输出：
  #author.all:从2015-01-01到2015-03-01之间公司调研栏目下的所有研报的作者，是字符串向量
  author.all <- NULL
  for(temp1 in temps){
    temp2 <- strsplit(temp1,"\r\n")[[1]] #以换行符分割
    autadr <- temp2[grep("td width", temp2)+4] # 提取author的行
    
    author1 <- NULL
    author <- gregexpr("title.*[\u4e00-\u9fa5]..>",autadr)
    for(i in 1:length(autadr))
    {
      a <- author[[i]]
      author1[i] <- substring(autadr[i], a+7, a+attr(a,"match.length")-3)
    }
    author.all <- c(author.all,author1)
  }
  return(author.all)
}

# ----------------- 提取评级的字段 -----------------

fetch.rate <- function(temps)
{
  #本函数用于从网页信息代码中提取研报的评级
  #
  #输入：
  #temps:网页信息代码，长度为80的字符向量
  
  #
  #输出：
  #rate.all:从2015-01-01到2015-03-01之间公司调研栏目下的所有研报的评级，是字符串向量
  rate.all <- NULL
  for(temp1 in temps){
    temp2 <- strsplit(temp1,"\r\n")[[1]] #以换行符分割
    ratadr <- temp2[grep("td width", temp2)+6] # 提取rate的行
    
    rate <- gregexpr(">.*<",ratadr)#也可以这么写rate <- gregexpr(">[\u4e00-\u9fa5]+.*<",ratadr)
    rate1 <- NULL
    for(i in 1:length(ratadr))
    {
      r <- rate[[i]]
      rate1[i] <- substring(ratadr[i], r+1, r+attr(r,"match.length")-2)
    }
    rate.all <- c(rate.all,rate1)
  }
  return(rate.all)
}



# ------------- 把数据以给定变量名称存进数据框里 ----------

loaddata <- function(title.all,author.all,nature.all.rate.all)
{
  #本函数用于把提取字段以数据框形式存储起来
  #
  #输入：
  #title.all:包含所有研报标题的字符串向量
  #author.all:包含所有研报作者的字符串向量
  #nature.all:包含所有研报属性的字符串向量，这里一律为“公司调研”
  #rate.all:包含所有研报评级的字符串向量
  
  #
  #输出：
  #company:包含所有字段的数据框，列分别为：
  # $F_CODE:证券代码
  # $F_NAME:证券简称
  # $F_DATE:公布日期
  # $F_0001:机构名称
  # $F_0002:报告名称
  # $F_0003:研报作者
  # $F_0004:研报类型
  # $F_0005:研报评级
  
  #把研报大标题以“-”分隔符分隔成不同字段
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
  return(company)
}

#----------数据保存函数---------------
data.save <- function(conn, company)
{
  #本函数用于把数据框保存进数据库
  #
  #输入：
  #company:一个数据框
  
  #无输出
  for (m in 1:nrow(company)){
    str <- "insert into TEST(F_CODE,F_NAME,F_DATE,F_0001,F_0002,F_0003,F_0004,F_0005) VALUES("
    for (n in 1:7){
      str <- paste(str, "'", company[m,n], "'", ",", sep='')
    }
    str <- paste(str,"'", company[m,8], "'", ")", sep='')
    sqlQuery(conn, str)
  }
}



#------------执行部分-----------
dayfirst <- "2015-01-01"
daylast <- "2015-03-01"
N=5
urllist<- getURL_list(dayfirst,daylast)
logRecTxt('test.txt', '1234')
temps <- readURL1(urllist, N)
title.all <- fetch.title(temps)
nature.all <- fetch.nature(temps)
author.all <- fetch.author(temps)
rate.all <- fetch.rate(temps)
company <- loaddata(title.all,author.all,nature.all.rate.all)
data.save(conn, company)
