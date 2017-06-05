module Main where

import CSVHTTP
import CSVDATA
import CSVDB
import System.Environment
import Database.HDBC
import Data.Maybe
import System.Locale hiding (defaultTimeLocale)
import Data.Time
import Data.Time.Format
import Network.Socket(withSocketsDo)
-- |The main function provides different functionalities:
--
--  [@addcomps@] will provide a list of companies to be added into the db
--
--  [@updatecomp@] will download a html file and parse it to get a new .csv file string to add to the db
--
--  [@parsestock] Displays companies from the db, to the terminal
--
--  [@showcompanies] Displays companies from the db, to the terminal
--
--  [@showstock@]   Displays all stock items for a given company
--
--  [@deletecompany@]  Removes a given company by it's ID
--
--  [@pricebycomp@]   Displays the Close, Date and Name for a given company
--
--  [@cheapestprice@]   Displays the cheapest close price for all companies
--
--  [@highestprice@]   Displays the highest close price for all companies
--
--  [@highestpriceall@]           Displays the highest close price for all companies
--
--  [@cheapestpriceall@]     Displays the cheapest close price for all companies
--
--  [@avg@]  Calcuates the avg price,for all companies
--
--  [@avgprice@]   Calculates the avg price by specifiyed year range, for all companies
--
--  [@estmoneymade@]   Calculates the estimated money made for a given comapny
--
--  [@avgpricebymonth@]   Calculates the avg price by month, group by year and company
--
--  [@avgpricebyyear@]   Calculates the avg price by year, group by year and company
--
--  [@getcsvlinks@]   Retrieves all csv links

main = withSocketsDo $ handleSqlError $
    do args <- getArgs
       conn <- connect "stockDB.db"
       --addCompanies conn companyList
       case args of
         ["addcomps"] -> addcomps conn
         ["updatecomp",id] -> updatecomp conn id
         ["parsestock",id] -> parsestock conn id
         ["showcompanies"] -> showcompanies conn
         ["showstock",id] -> showstock conn id
         ["deletecompany",id] -> deletecompany conn id
         ["pricebycomp",id] ->pricebycomp conn id
         ["pricebycomp",id,fromDate,toDate] -> pricebycompdr conn id fromDate toDate
         ["cheapestprice",id] -> cheapestprice conn id
         ["cheapestprice",id,fromDate,toDate] -> cheapestpricedr conn id fromDate toDate
         ["highestprice",id] -> highestprice conn id
         ["highestprice",id,fromDate,toDate] -> highestpricedr conn id fromDate toDate
         ["highestpriceall"] -> highestpriceall conn
         ["highestpriceall",fromDate,toDate] -> highestpricealldr conn fromDate toDate
         ["cheapestpriceall",fromDate,toDate] -> cheapestpricealldr conn fromDate toDate
         ["avg"] -> avgall conn
         ["avgprice",fromDate,toDate] -> avgpricedr conn fromDate toDate
         ["estmoneymade", id] -> estmoneymade conn id
         ["avgpricebymonth", id] -> avgpricebymonth conn id
         ["avgpricebyyear"] -> avgpricebyyear conn
         ["getcsvlinks"] -> getcsvlinks
         _ -> print WrongArg   	
       disconnect conn

     

									 
getcsvlinks = 
    do  
      	let yahooLink = "http://finance.yahoo.com/q/hp?a=&b=&c=&d=11&e=18&f=2015&g=d&s="++ head(stocksToPull) ++"%2C+&ql=1"
      	htmltext <- downloadURL2(yahooLink)
      	case htmltext of
      	  Left l -> error "problem parsing" $ show l
      	  Right r ->
      	  		   do
      	  			  let urllist = parseURLs $ r
      	  			  let newlist = getLink $ urllist
      	  			  print newlist
       
       
   
addcomps conn = 
    do 
    	putStrLn "Adding data..."
    	addCompanies conn companyList

        
updatecomp conn id = 
    do  
	   let a = checkResult $ convert id
	   maybecomp <- getCompany conn a
	   let comp =fromJust maybecomp
	   
	   --download link and update url
	   htmltext <-downloadURL2 $ link comp
	   case htmltext of
	     Left l -> error "Problem parsing" $ (show l)
	     Right r ->  
         		  do  
         		     let urlList = parseURLs r
         		     let newurl = getLink urlList
         		     updateCompany conn a newurl
         		     commit conn
         		  
         		    

       
deletecompany conn id = 
    do 
       let a = checkResult $ convert id
       removeCompany conn a
       commit conn
       
prompt conn =
	do
	   errorStr <- printString conn
	   putStrLn errorStr
   
showcompanies conn =
	do clist <- getCompanies conn
	   --commit conn
	   putStrLn $ show clist
	  
	   
showstock conn id =
	do 
	   let a = checkResult $ convert id
	   maybecomp <- getCompany conn a
	   let comp =fromJust $ maybecomp
	   let i = identifier comp
	   
	   stocklist <- getStock conn i
	   --commit conn
	   print stocklist
    
parsestock conn id = 
    do
       let a = checkResult $ convert id
       maybecomp <- getCompany conn a
       let comp =fromJust $ maybecomp
       let u=  url comp
       let id =show $ identifier comp
       
       mys <-downloadURL2 u
       case mys of
         Left x ->  error "Problem parsing" $ (show x)
         Right r -> 
         		 do 
         		    let lists =  parseCSV r
         		    case lists of
         		     Left a -> error "Problem parsing" $ (show a)
         		     Right b -> 
         		     		do
         		     			let recordsList=removeHeader(justTheList(lists))
         		     			let stockPrices = addKey recordsList [id]
         		     			--print stockPrices
         		     			let listofstock =map convertToStock stockPrices
         		     			addStock conn listofstock
         	
         		     			
pricebycomp conn id = 
    do 
       let a = checkResult $ convert id
       printpbc conn a
       
pricebycompdr conn id fromDate toDate = 
    do 
       let a = checkResult $ convert id
       printpbcdr conn a fromDate toDate
       
       
cheapestprice conn id = 
    do 
       let a = checkResult $ convert id
       printcheapest conn a
 
cheapestpricedr conn id fromDate toDate = 
    do 
       let a = checkResult $ convert id
       printcheapestdr conn a fromDate toDate       
              
highestprice conn id = 
    do 
       let a = checkResult $ convert id
       printhighest conn a
       
highestpricedr conn id fromDate toDate = 
    do 
       let a = checkResult $ convert id
       printhighestdr conn a fromDate toDate    
       
cheapestpricealldr conn fromDate toDate = 
    do 
       printcheapestpricealldr conn fromDate toDate    
       
highestpriceall conn = 
    do printhighestpriceall conn
              
highestpricealldr conn fromDate toDate = 
    do 
       printhighestalldr conn fromDate toDate        

avgall conn = 
    do printavgall conn
    
avgpricedr conn fromDate toDate = 
    do 
       printavgpricedr conn fromDate toDate        

       
estmoneymade conn id = 
    do 
       let a = checkResult $ convert id
       printestmoneymade conn a
       
avgpricebymonth conn id = 
    do 
       let a = checkResult $ convert id
       printavgpricebymonth conn a
       
avgpricebyyear conn = 
    do printavgpricebyyear conn
       
      
       
       
--get company data functions 
    
stocksToPull = "GOOG":"BAC":"MSFT":"TXN":"GOOG":"FB":"YHOO":"TWTR":[]       

csvlinksToList :: [String] -> [String]
csvlinksToList [] = []
csvlinksToList (x:xs) =
      	let yahooLink ="http://real-chart.finance.yahoo.com/table.csv?s="++x++"&a=1&b=5&c=1971&d=10&e=24&f=2015&g=d&ignore=.csv"
      		in yahooLink:csvlinksToList(xs)
      		
csvlinksToCompany :: [String] -> [String] -> Int ->[Company]
csvlinksToCompany [] [] _ = []
csvlinksToCompany (x:xs)(y:ys) i =
      	let yahooCsvURL =y
            yahooCode =x
            link = ""
            id=i
        in Company {identifier = id, name =yahooCode,url=yahooCsvURL,link=link}:csvlinksToCompany xs ys (i+1)
csvlist =  csvlinksToList $ stocksToPull
companyList = csvlinksToCompany stocksToPull csvlist 1