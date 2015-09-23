# ----------groundworks---------------
rm(list=ls())
library("RCurl")
library("RODBC")
conn <- odbcConnect("TXDB",uid="haquser01",pwd="123456",case="tolower",believeNRows=FALSE)
#若不清楚数据源名称，可使用函数odbcDataSources()


#myheader <- c(
# "User-Agent"="Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2272.89 Safari/537.36",
#"Accept"="text/css,*/*;q=0.1",
#"Accept-Language"="zh-CN,zh;q=0.8",
#"Connection"="keep-alive"
#)

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
# 拼接得到网址url主要部分
getURL_list <- function(urllist,page){
  #经查询，microbell网站公司调研板块自2015-01-01至2015-03-01共有80个页面，故设定为
  #urllist=0
  #page=1:80
  urllist[page]=paste("http://www.microbell.com/superareport.asp?dockey=%25&dockey2=&doctime1=2015-01-01&doctime2=2015-03-01&report=1&doctype=DocTitle&paget=",page,"&from=",sep='')
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
  # temp：上榜日的网页信息代码，一个字符向量
  
  
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

fetch.title <- function(temps){
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

fetch.nature <- function(temps){
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

fetch.author <- function(temps){
  author.all <- NULL
  for(temp1 in temps){
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

fetch.rate <- function(temps){
  rate.all <- NULL
  for(temp1 in temps){
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
loaddata <- function(title.all,author.all,nature.all.rate.all){
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
}






  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  