#----------groundworks---------------
rm(list=ls())
library("RCurl")
library("RODBC")
conn =odbcConnect("txdb", uid="haquser01", pwd="123456", believeNRows=FALSE)
# creat_table <- "create table T_STOCK_LHB2(SECUCODE varchar2(10),SECUNAME varchar2(20),TRADINGDATE varchar2(20),ABNORTYPE  varchar2(200),TURNVOLUMN  number(20,4),TRUNVAL  number(20,4),SALDEPNAME  varchar2(100),TRADESIGNAL  varchar2(1),TRADRANK  number(3),BUYSUM  number(20,4),SALESUM  number(20,4),constraint TH_LH PRIMARY KEY (SECUCODE,SECUNAME,TRADINGDATE,SALDEPNAME,ABNORTYPE,TRADESIGNAL,TRADRANK))"
# sqlQuery(conn,creat_table)



#-------------日志存储函数----------
logRecTxt = function(fileName, log)
{
  # 本函数将日志保存为文本格式
  #   log = '1234'
  #   fileName = 'test.txt'
  
  log = paste(Sys.time(), log, sep=",")
  write.table(log, fileName, quote=F, sep=",",append=T,col.names=F,row.names=F)
}


#-------------获取日期列表函数---------
get_Datelist <- function(channel,TradeDate)
{
  # 本函数用于获取数据库最新记录与当前之间的所有日期
  #
  # 输入：
  # channel: 数据库连接，ODBCconn结果
  # TRADEDATE:当前交易日期，日期格式
  
  #
  # 输出：
  # datelist: 所有间隔日期，字符串向量,"%Y-%m-%d"
  
  # 获得数据库记录的最后日期
  statement_Date <- "select max(F_DATE) from T_STOCK_LHB" 
  latest_date <- sqlQuery(channel,statement_Date) 
  
  # 获得间隔所有日期
  date01 <- as.Date(as.character(latest_date),"%Y%m%d") - 1  # 比最后日期向前拓一天
  n <- as.double(TradeDate-as.double(date01))
  
  # 当前交易日在最后记录之前或之后
  if(n > 0)
  {
    dateNum <- 1:n
    datelist <- format(dateNum+date01,"%Y-%m-%d") 
  }else if(n == 0)
  {
    datelist <- date01
  }else
  {
    datelist <- NULL
  }
  return(datelist)
}


#------------获取代码列表函数--------
get_CODE <- function(datelist,N)
{ 
  # 本函数用于读取给定日期列表每一天上榜股票代码
  
  # 输入：
  # datelist: 需要获取代码的日期，一个向量"%Y-%m-%d"
  # N: 尝试打开主页面url的次数，一个整数
  
  #
  # 输出：
  # datecode: 日期和对应上榜股票代码，一个数据框，打开或读取不成功时长度为零
  date = c()
  code = c()
  for(i in 1:length(datelist))
  {
    TRADEDATE = datelist[i]
    
    # 对榜单网页代码进行拆分，读取表单每一行中包含的股票代码
    
    sub_url01 <- readURL1(TRADEDATE,N)                    # 试开N次
    if(!is.null(sub_url01))
    {
      sub_url02 <- strsplit(sub_url01,"</tr><tr")[[1]]     # 按行分割
      h1 <- grep("class=\"all.{+}>\\d{6}<",sub_url02)        # 分出带有股票代码的部分
      if(length(h1)==0)                                 # 若该日没有股票代码则进行下一天   
      {
        next
      }
      codelist=getDATAlr(sub_url02,">\\d{6}<",h1,1,-1,-1)
      date1 <- rep(TRADEDATE,length(codelist))
      code1 <- codelist
      date <- c(date, date1)
      code <- c(code, code1)
      
    }
    else
    {
      next            # 若打开网页不成功则进行下一天，试开函数内对打不开主网页进行了记录
    }
    datecode <- cbind(code,date)
  }
  datecode <- cbind(code,date)
  return(datecode)               
}


#-----------试开url函数1-----------
readURL1 <- function(TRADEDATE, N)
{
  # 本函数用于试开主榜单网页并得到代码
  #
  # 输入：
  # TRADEDATE:交易日，一个字符串，"%Y-%m-%d"
  # N :尝试打开次数，一个整数
  
  #
  # 输出：
  # url：上榜日的网页信息代码，一个字符向量
  
  # 拼接得到网址url主要部分
  # 读取不成功，则反复读取N次。若N次读取不成功则返回NULL，记录错误日志。
  main_address <- paste("http://data.eastmoney.com/stock/lhb/", TRADEDATE, ".html", sep='')
  url=tryCatch( {getURL(main_address,.encoding="GBK")},
                error=function(err){
                                     url = NULL
                                   })
  j = 1
  while(is.null(url))
  {
    if(j>N)
    {
      logRecTxt("test.txt",paste("打不开主网页",TRADEDATE))
      break
    }
    url=tryCatch( {getURL(main_address,.encoding="GBK")},
                  error=function(err){
                    url = NULL
                  })
    j = j+1
  }
  return(url)
}


#------------试开url函数2---------------
readURL2 <- function(CODE,TRADEDATE,N)
{
  # 本函数用于试开详情页面并得到代码
  #
  # 输入：
  # CODE：股票代码，一个字符串
  # TRADEDATE:交易日，一个字符串
  # N :尝试打开次数，一个整数
  
  #
  # 输出：
  # url：交易详情网页信息代码，一个字符向量
  
  # 拼接得到网址url主要部分
  # 读取不成功，则反复读取N次。若N次读取不成功则返回NULL，记录错误日志。
  main_address <- paste("http://data.eastmoney.com/stock/lhb,",TRADEDATE,",",CODE,".html",sep='')
  url=tryCatch( {getURL(main_address,.encoding="GBK")},
                error=function(err){
                  url = NULL
                })
  j = 1
  while(is.null(url))
  {
    if(j>N)
    {
      logRecTxt("test.txt",paste("打不开明细网页",TRADEDATE,CODE))
      break
    }
    url=tryCatch( {getURL(main_address,.encoding="GBK")},
                  error=function(err){
                    url = NULL
                  })
    j = j+1
  }
  return(url)
}



#------------截取数据函数---------------
getDATAlr <- function(suburl, pattern2, g1, po, left, right)
{
  # 本函数识别并提取匹配子字符串
  #
  # 输入：
  # suburl:原始字符串，一个向量
  # pattern2:要匹配的正则表达式，一个字符串
  # g1: 从原始字符串选取目标字符串的位置,一个整数向量
  # po: 一个元素有多处匹配时，截取第几处匹配的信息，一个整数
  # left：从匹配起点的左侧第几个位置开始读取，一个整数
  # right： 到匹配终点向右第几位结束读取，一个整数
  
  #
  # 输出：
  # g4：提取结果，一个字符向量
  
  # 取出列
  g2 <- suburl[g1]  
  # 匹配，生成结果列表g3,g3的每个元素为g2的对应字符串匹配pattern2的起始位置和长度
  g3 <- gregexpr(pattern2,g2 )
  g4 <- c()  # 初始化g4
  for (i in 1:length(g3))
  {
    if(g3[[i]][1]!=-1)
    {
      g4[i] <- substring(g2[i],g3[[i]][po]-left,g3[[i]][po]-1+attr(g3[[i]],"match.length")[po]+right)
    }
    # 匹配结果存入g4
  }
  return(g4)
} 



#------------提取表格函数-------------
Extractable = function(suburl03,i)
{
  # 本函数用于从详情页面提取前五名表格
  # 
  # 输入：
  # suburl03: 包含所有ABNORTYPE的源代码，一个字符串向量
  # i: 提取的是第几个ABNORTYPE，一个整数
  
  #
  # 输出：
  # doubletab:一个列表，元素为两个数据框，分别为买入和卖出表格
  
  suburl04=c()
  # we get two top5 tables of BUY and SALE of each ABNORTYPE
  suburl04[1] <- getDATAlr(suburl03,"<tbody>.*?</tbody>",i,1,0,-6)
  suburl04[2] <- strsplit(getDATAlr(suburl03,"<tbody>.*?</tbody>",i,2,0,-6),"style=")[[1]][1]
  suburl051 <- as.data.frame(strsplit(suburl04[1], "</tr><tr")[[1]])
  suburl052 = as.data.frame(strsplit(suburl04[2],"</tr><tr")[[1]])
  doubletab = list(suburl051,suburl052)
  return(doubletab)
}


#------------逐行读取函数-----
parseline = function(table,j)
{
  h1 = seq(dim(table)[1])
  suburl05 = table[,1]
  SELDEPNAME = getDATAlr(suburl05,".html\">\\D{+}<",h1,1,-7,-1)
  n = length(SELDEPNAME)
  if(n==0)
  {
    return(NULL)
  }
  else
  {
    h2 <- seq(n)
    BUYSUM01 <- getDATAlr(suburl05, "(>\\d{1,10}\\.\\d{1,5}%?<)|(>-<)", h2, 1, -1, -1)
    SALESUM01 = getDATAlr(suburl05, "(>\\d{1,10}\\.\\d{1,5}%?<)|(>-<)", h2, 3, -1, -1)
    BUYSUM01[BUYSUM01=="-"] = NA
    SALESUM01[SALESUM01=="-"] = NA
    # 生成买卖标志
    ifelse(j==1,TRADESIGN01 <- rep("B", n),TRADESIGN01 <- rep("S", n))
    # 生成营业部排名
    TRADERANK01 <- h2
    sumtab = cbind(SELDEPNAME, TRADESIGN01,TRADERANK01,BUYSUM01, SALESUM01)
    return(as.data.frame(sumtab))
  }
}



#------------爬虫函数------------
top5Parser=function(CODE, TRADEDATE, url)
{
  # 本函数用于读取每只上榜股票的交易明细信息
  
  #
  # 输入：
  # CODE：股票代码，一个字符串
  # TRADEDATE：上榜日期，一个时间格式字符串，格式为%Y-%m-%d
  # url: 记录该股票上榜日交易明细网页代码，一个字符串
  
  #
  # 输出：
  # lhrank: 该日该股票交易明细，一个数据框
  
  pattern_abnortype01<-"<li>类型"
  pattern_abnortype02<-"<li>类型.{+}<"
  
  suburl02 <- strsplit(url,"<div")[[1]]     # seperate the url into blocks
  g1 <- grep(pattern_abnortype01, suburl02)
  ABNORTYPE0 <- getDATAlr(suburl02, pattern_abnortype02, g1, 1, -7, -1)  # get abnormaltype from relevant blocks
  suburl03 <- suburl02[g1]  # here suburl03 represents two blocks including abnormaltypes
  
  # 初始化向量
  LT = length(g1)
  typetab = data.frame()
  for(i in 1:LT)  # 每个类型进行循环
  { 
    doubletab = Extractable(suburl03,i)
    sumtab = data.frame()
    for(j in 1:2)
    {
      table = doubletab[[j]]
      sumtab = rbind(sumtab,parseline(table,j))
    }
    typelen = dim(sumtab)[1]
    # 读取每个类型的成交额和总成交量
    TURNVOL1 = rep(getDATAlr(suburl03, "\\d{1,6}.\\d{+}万手",i,1,0,-2),typelen)
    TURNVAL1 = rep(getDATAlr(suburl03, "\\d{1,10}.\\d{+}万元",i,1,0,-2),typelen)
    
    # 组织类型数据
    ABNORTYPE1 = rep(ABNORTYPE0[i],typelen)
    typetab = rbind(typetab,cbind(ABNORTYPE1,TURNVOL1,TURNVAL1,sumtab))
    #ABNORTYPE <- c(ABNORTYPE, ABNORTYPE1)
    #BUYSUM <- c(BUYSUM, BUYSUM01,BUYSUM02)
    #SALESUM = c(SALESUM, SALESUM01,SALESUM02)
    #TRADESIGN = c(TRADESIGN, TRADESIGN01,TRADESIGN02)
    #TRADERANK = c(TRADERANK, TRADERANK01,TRADERANK02)
    #TURNVOL = c(TURNVOL, TURNVOL1)
    #TURNVAL = c(TURNVAL, TURNVAL1)
    #SELDEPNAME = c(SELDEPNAME, SELDEPNAME1,SELDEPNAME2)
  }
  
  # 组织个股数据
  xq=dim(typetab)[1]
  SECUCODE = rep(CODE,xq)
  SECUNAME = rep(getDATAlr(url,"<title>\\D{+}.\\d",1,1,-7,-2),xq)
  TRADINGDATE = rep(format(as.Date(TRADEDATE), "%Y%m%d"), xq)
  LHRANK <- cbind(SECUCODE, SECUNAME, TRADINGDATE, typetab)
  return(LHRANK)
}



#----------数据保存函数---------------
data.save <- function(channel, LHRANK)
{
  # 本函数用于保存龙虎榜数据到数据库中
  
  # 输入：
  # channel：到数据库的链接，一个ODBCconn函数的结果
  # LHRANK：要保存的龙虎榜数据表格，一个数据框
  
  # 无输出
  for (m in 1:dim(LHRANK)[1])
  {
    # 把每行内容按照数据库指令格式写为一个字符串
    str1 <- "insert into T_STOCK_LHB2
    (SECUCODE, SECUNAME, TRADINGDATE, ABNORTYPE, TURNVOLUMN, TRUNVAL, 
    SALDEPNAME,TRADESIGNAL,TRADRANK ,BUYSUM,SALESUM) values("
    for (n in 1:10)
    {
      str1 <- paste(str1, "'", LHRANK[m,][[n]], "'", ",", sep='')
    }
    str1 <- paste(str1, "'", LHRANK[m,][[11]], "'", ")", sep='')
    sqlQuery(channel, str1) 
  }  
}


#---------东方财富龙虎榜主函数----------
getLHBData <- function(datelist,SECUCODE=NULL,N)
{
  # 本函数用于读取给定日期列表的龙虎榜详情数据，或用于再次核查自身读取失败日期与代码的龙虎榜数据
  
  #
  # 输入：
  # datelist: 需要更新或读取的日期列表，一个%Y-%m-%d格式的字符串向量
  # SECUCODE：需要再次核实的代码列表，一个字符串向量，缺省为空值，不为空时需与datelist等长
  # N：每次试开次数，一个整数
  
  #
  # 输出：
  # resultblog:一个列表，元素分别为：
  #   $lhrank:读取成功的龙虎榜数据，一个数据框
  #   $date1: 未读取股票代码数据的日期，一个字符串向量
  #   $date2: 未读取龙虎榜交易详情信息的日期，一个字符串向量
  #   $code2: 未读取龙虎榜交易详情信息的股票代码，一个字符型向量
  
  # initializing the variables
  code_1 <- character()
  datelist_1 <- character()
  datelist_2 <- character()
  lhrank2 = list()
  codedate = data.frame()
  if(is.null(SECUCODE))
  {
    # 获取日期与股票代码联合列表
    for(i in 1:length(datelist))
    {
      # 对每日进行读取股票列表的尝试
      codedate1 = get_CODE(datelist[i])
      if(is.null(codedate1))            # 若当天无龙虎榜数据或打开页面失败则记录并进行下一天
      {
        datelist_1 = c(datelist_1, datelist[i])
        next
      }
      
      else
      {
        codedate = rbind(codedate, codedate1)
      }
    }
  }
  else                               # 若调用时SECUCODE有赋值则直接组合成数据框
  {
    codedate = cbind(SECUCODE, datelist)
  }
  if(is.null(codedate))                    # 若全部日期均不能读出个股代码则终止函数
  {
    print("日期代码无效，请检查")
    return()
  }
  if(dim(codedate)[1]!=0)                 # 日期代码表有内容时进行详情读取
  {
    for(f in 1:dim(codedate)[1])
    {
      # 读取详情页面代码
      CODE = as.character(codedate[f,1])
      TRADEDATE = as.character(codedate[f,2])
      url = readURL2(CODE,TRADEDATE,N)
      if(!is.null(url))                    # 代码下载成功则尝试进行爬虫
      {
        lhrank1 = tryCatch({ 
                              top5Parser(CODE,TRADEDATE,url)
                           },
                           error=function(err){
                                                lhrank1 <-NULL
                                              })
        if(is.null(lhrank1))
        {
          logRecTxt("test.txt",paste(TRADEDATE,CODE,"有效网址爬虫失败，请检查函数"))
          datelist_2 <- c(datelist_2,TRADEDATE)  # N次均不成功则记录该日日期并尝试下一行
          code_1 <- c(code_1,CODE)
        }
        else
        {
          t = length(lhrank2)
          lhrank2[[t+1]] <- lhrank1  
        }
      }
      else                                       # 读取详情页面失败，记录并进行下支个股
      {
        datelist_2 <- c(datelist_2,TRADEDATE)  
        code_1 <- c(code_1,CODE)
        next
      }
    }
    resultblog = list(lhrank=lhrank2,date1=datelist_1,date2=datelist_2,code2=code_1)
    return(resultblog)
  }  
  else
  {
    print("日期代码无效，请检查")
    return()
  }
  
}





#------------执行部分-----------
file8 = "getLHBDatanew6.out"
# 更新数据并进行动态分析
datelist = get_Datelist(conn,Sys.Date())
a1 = Sys.time()
Rprof(file8)
lhbDatablog = getLHBData(datelist,NULL,5)
a2 = Sys.time()
Rprof(NULL)

r1 = lhbDatablog
r2 = lhbDatablog
lhbData = lhbDatablog$lhrank
N = 8                                                                                                                            
j = 1

# 对股票代码读取错误的日期进行循环尝试，若超过限定次数而仍然错误，则打印并终止
b1=Sys.time()
while(length(r1$date1)!=0)
{ 
  if(j>N)
  {
    print(r1$date1)
    break
  }
  r1 = getLHBData(r1$date1,NULL,5)
  if(!is.null(r1))
  {
    lhbData = c(lhbData,r1$lhrank)
  }
  j = j+1
}
b2=Sys.time()

# 对无法读取详情的日期和代码进行检查，若超过限定次数而仍然错误，则打印并终止
j = 1
c1 = Sys.time()
while(length(r2$date2)!=0)
{ 
  if(j>N)
  {
    print(r2$date2)
    break
  }
  r2 = getLHBData(r2$date2,r2$code2,5)
  if(!is.null(r2))
  {
    lhbData = c(lhbData,r2$lhrank)
  }
  j = j+1
}
c2 = Sys.time()

# 集合多次复查数据进行存储
d1 = Sys.time()
for(h in 1:length(lhbData))
{
  data.save(conn,lhbData[[h]])
}
d2 = Sys.time()
odbcClose(conn)






#------------test---------
CODE = "002249"
TRADEDATE = "2015-07-14"
url = readURL2(CODE,TRADEDATE,3)
tmp = getLHBData(TRADEDATE,CODE,5)$lhrank[[1]]
CODE2 = "600100"
TRADEDATE2 = "2015-07-10"
tmp2 = getLHBData(TRADEDATE2,CODE2,5)$lhrank[[1]]
CODE3 = "900928"
TRADEDATE3 = "2015-07-10"
tmp3 = getLHBData(TRADEDATE3,CODE3,5)$lhrank[[1]]
CODE4 = "000558"
TRADEDATE4 = "2015-07-14"
tmp3 = getLHBData(TRADEDATE4,CODE4,5)$lhrank[[1]]
tic = Sys.time()
LHBLOG = getLHBData("2015-07-10",NULL,5)
tic2 = Sys.time()


