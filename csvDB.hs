-- | Method for handling Database connectivity and queries.
-- | Some methods were used and modified according to Chapter 21 of 'Real World Haskell'
-- |  by Bryan O'Sullivan, Don Stewart, and John Goerzen.
-- | Link :http://book.realworldhaskell.org/read/extended-example-web-client-programming.html

module CSVDB where

import CSVDATA
import Database.HDBC
import Database.HDBC.Sqlite3
import Control.Monad(when)
import Data.List(sort)


              
-- | Initialise DB and return database Connection
connect :: FilePath -> IO Connection
connect fp =
    do conn <- connectSqlite3 fp
       prepDB conn
       return conn
       
       
{- | Prepare the  db for adding two tables.

two tables are created and data is verified
* C_ID is unique
-}
prepDB :: IConnection conn => conn -> IO ()
prepDB conn =
    do tables <- getTables conn
       when (not ("Companies" `elem` tables)) $
           do run conn "CREATE TABLE Companies (\
                       \C_ID INTEGER NOT NULL PRIMARY KEY,\
                       \Name VARCHAR(40),\
                       \URL VARCHAR(120),\
                       \Link VARCHAR(120),\
                       \UNIQUE(C_ID))" []
              return ()
       when (not ("Prices" `elem` tables)) $
           do run conn "CREATE TABLE Prices (\
                       \P_ID INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,\
                       \Date Date,\
                       \Open VARCHAR(40),\
                       \High VARCHAR(40),\
                       \Low VARCHAR(40),\
                       \Close VARCHAR(40),\
                       \Volume bigint(11),\
                       \AdjClose VARCHAR(40),\
                       \COMP_ID INTEGER,\
                       \FOREIGN KEY(COMP_ID) REFERENCES Companies(C_ID))" []
              return ()
       commit conn 
       
       
       
       

    
    
    
{- | Adds many stock items to the database.
Each item represents a line from the csv file.
The entire csv file will be represented by many stock items.
Each item also has a key, which represents the company it belongs to -}    
addStock :: IConnection conn => conn -> [Stock] -> IO ()
addStock conn [] = return()
addStock conn s =
      do stmt <- prepare conn "INSERT INTO Prices(Date,Open,High,Low,Close,Volume,AdjClose,COMP_ID) VALUES (?,?,?,?,?,?,?,?)"
         executeMany stmt (map convStockToList s)
         commit conn
 
         
         
{- | Adds many companies to the db
Each company stores it's csv url and a link to it's html page-}    
addCompanies :: IConnection conn => conn -> [Company] -> IO ()
addCompanies conn [] = return()
addCompanies conn s =
      do stmt <- prepare conn "INSERT INTO Companies(Name,URL,Link) VALUES (?,?,?)"
         executeMany stmt (map convCompToList s)
         commit conn
         
{- | An attempt to add a new company to the db. -}
addCompany :: IConnection conn => conn -> Company -> IO Company
addCompany conn c = 
    handleSql errorHandler $
      do -- Insert the contents of a company into the table.  The database
         -- will automatically assign a  C_ID.
         run conn "INSERT INTO Companies (C_ID,Name,URL,Link) VALUES (?,?,?,?)"
             [toSql(identifier c),toSql (name c),toSql (url c),toSql(link c)]
         -- This verifies that a record has been entered by checking it's C_ID
         r <- quickQuery' conn "SELECT C_ID FROM Companies WHERE C_ID = ?"
              [toSql (identifier c)]
         -- Conditions for record
         commit conn
         case r of
           [[x]] -> return $ c {identifier = fromSql x}
           y -> fail $ "addCompany: Empty List: " ++ show(y)
    where errorHandler e = 
              do fail $ "Error adding Company; Record may already exist?\n"
                     ++ show e   
                     
{- | Gets a list of all companies. -}
getCompanies :: IConnection conn => conn -> IO [Company]
getCompanies conn =
  handleSql errorHandler $
      do res <- quickQuery' conn "SELECT * FROM Companies" []
         -- Conditions for record
         commit conn
         case res of
          [x] -> return (map convCompanyRow res)
          [] -> return (error "empty list")
   where errorHandler e = 
              do fail $ "Error retrieving Company; Record may not exist?\n"
                     ++ show e   
         
       
{- | Remove a Company. This also has to delete any stock which is assosiated with the company. -}
removeCompany :: IConnection conn => conn -> Int -> IO ()
removeCompany conn id =
    do run conn "DELETE FROM Companies WHERE C_ID = ?" 
         [toSql id]
       run conn "DELETE FROM Prices WHERE COMP_ID = ?"
         [toSql id]
       commit conn
       return ()     
       
       
       
{- | Updates an existing company. A more current csv link is added
--However all the previous stock relating to the old link is deleted-}
updateCompany :: IConnection conn => conn -> Int -> String -> IO ()
updateCompany conn id newurl =
   do run conn "UPDATE Companies SET URL = ? WHERE C_ID = ?" 
         [toSql newurl,toSql id]
      run conn "DELETE FROM Prices WHERE COMP_ID = ?"
         [toSql id]
      commit conn
      return ()
      
      
 
{- | Get a particular Company, by it's id. -}
getCompany :: IConnection conn => conn -> Int -> IO (Maybe Company)
getCompany conn wantedId =
    do res <- quickQuery' conn 
              "SELECT * FROM Companies WHERE C_ID = ?" [toSql wantedId]
       commit conn
       case res of
         [x] -> return (Just (convCompanyRow x))
         [] -> return Nothing
         x -> fail $ "More than one record with same ID"
         
         
         
         
{- | Gets stock from one company. The ID is required to return the stock. -}
getStock :: IConnection conn => conn -> Int ->  IO [Stock]
getStock conn c =
    do res <- quickQuery' conn 
              "SELECT * FROM Prices WHERE COMP_ID = ?" [toSql c]
       commit conn
       case res of
         [x] -> return (map convStockRow res)
         [] -> return (error "empty list")
   where errorHandler e = 
              do fail $ "Error retrieving Company; Record may not exist?\n"
                     ++ show e  
         

         
         
         
       
--Methods for data type and sqlvalue conversion----------------------------------------------------------



{- | Converts a list of SqlValues to a Stock data type -}
convStockRow :: [SqlValue] -> Stock
convStockRow (k:d:o:h:l:c:v:xs) =
    Stock {date = fromSql d,
           open = fromSql o,
           high = fromSql h,
           low = fromSql l,
           close = fromSql c,
           volume = fromSql v,
           adj = fromSql $ head xs,
           key = fromSql k}
convStockRow x = error $ "Can't convert stock row " ++ show x 


{- | Converts a list of SqlValues to a Company data type -}
convCompanyRow :: [SqlValue] -> Company
convCompanyRow (id:n:u:xs) =
    Company {identifier = fromSql id,
             name = fromSql n,
             url = fromSql u,
             link = fromSql $ head xs}
convCompanyRow x = error $ "Can't convert company row " ++ show x   


{- | Converts a Stock data type to a list of SqlValues for insertion into the DB. -}
convStockToList :: Stock -> [SqlValue]
convStockToList (Stock d o h l c v a k) =[toSql(d),
				    					  toSql(o),
				    					  toSql(h),
				    					  toSql(l),
				    					  toSql(c),
				    					  toSql(v),
				    					  toSql(a),
				    					  toSql(k)]
convStockToList (Stock _ _ _ _ _ _ _ _) = error $ "Can't convert stock row "  
         
         
{- | Converts a Company data type to a list of SqlValues for insertion into the DB. -}
convCompToList :: Company -> [SqlValue]
convCompToList (Company k n u l) =[toSql(n),
				   				   toSql(u),
				   				   toSql(l)]
convCompToList (Company _ _ _ _) = error $ "Can't convert company row "  



--SELECT * FROM Orders WHERE strftime('%Y-%m-%d',OrderDate) BETWEEN "1996-07-04" AND "1996-07-09"
--SQL Joins for prices, highest, cheapest and average----------------------------------------------------------


-- |Method to print all price closes, sorted by price and for one company
priceByComp :: IConnection conn => conn -> Int -> IO [[String]]
priceByComp conn id =
    do res <- quickQuery' conn "SELECT \
                       			\Prices.Close, Prices.Date,Companies.Name \
                       			\FROM Prices \
                       			\INNER JOIN Companies \
                       			\ON Prices.COMP_ID=Companies.C_ID \
                       			\WHERE Companies.C_ID=? \
                       			\ORDER BY Prices.Close desc" [toSql id]  
       commit conn
       return (map(map (\x -> fromSql x)) res)
       
       
-- |Method to print all price closes, sorted by price and for one company
priceByCompDateRange :: IConnection conn => conn -> Int -> String -> String -> IO [[String]]
priceByCompDateRange conn id fromDate toDate =
    do res <- quickQuery' conn "SELECT \
                       			\Prices.Close, Prices.Date,Companies.Name \
                       			\FROM Prices \
                       			\INNER JOIN Companies \
                       			\ON Prices.COMP_ID=Companies.C_ID \
                       			\WHERE Companies.C_ID=? AND strftime('%Y-%m-%d',Prices.Date) BETWEEN ? AND ? \
                       			\ORDER BY Prices.Close desc" [toSql id, toSql fromDate, toSql toDate]  
       commit conn
       return (map(map (\x -> fromSql x)) res)
    
        
       
       
-- |Method to print the highest price close, sorted by price and for one company
highestPrice :: IConnection conn => conn -> Int -> IO [[String]]
highestPrice conn id =
    do res <- quickQuery' conn "SELECT \
                       			\MAX(Prices.Close), Prices.Date,Companies.Name \
                       			\FROM Prices \
                       			\INNER JOIN Companies \
                       			\ON Prices.COMP_ID=Companies.C_ID \
                       			\WHERE Companies.C_ID=? \
                       			\ORDER BY Prices.Close desc" [toSql id]
       commit conn
       return (map(map (\x -> fromSql x)) res)
       
-- |Method to print the highest price close, sorted by price and for one company
highestPriceDateRange :: IConnection conn => conn -> Int -> String -> String -> IO [[String]]
highestPriceDateRange conn id fromDate toDate =
    do res <- quickQuery' conn "SELECT \
                       			\MAX(Prices.Close), Prices.Date,Companies.Name \
                       			\FROM Prices \
                       			\INNER JOIN Companies \
                       			\ON Prices.COMP_ID=Companies.C_ID \
                       			\WHERE Companies.C_ID=? AND strftime('%Y-%m-%d',Prices.Date) BETWEEN ? AND ? \
                       			\ORDER BY Prices.Close desc" [toSql id, toSql fromDate, toSql toDate]  
       commit conn
       return (map(map (\x -> fromSql x)) res)
       
       
-- |Method to print the highest price close, sorted by price and for all companies
highestPriceAll :: IConnection conn => conn -> IO [[String]]
highestPriceAll conn  =
    do res <- quickQuery' conn "SELECT \
                       			\MAX(Prices.Close), Prices.Date,Companies.Name \
                       			\FROM Prices \
                       			\INNER JOIN Companies \
                       			\ON Prices.COMP_ID=Companies.C_ID \
                       			\GROUP BY Prices.COMP_ID \
                       			\ORDER BY Prices.Close desc" []  
       commit conn                			
       return (map(map (\x -> fromSql x)) res)
       
-- |Method to print the highest price close, sorted by price and for all companies
highestPriceAllDateRange :: IConnection conn => conn -> String -> String -> IO [[String]]
highestPriceAllDateRange conn fromDate toDate  =
    do res <- quickQuery' conn "SELECT \
                       			\MAX(Prices.Close), Prices.Date,Companies.Name \
                       			\FROM Prices \
                       			\INNER JOIN Companies \
                       			\ON Prices.COMP_ID=Companies.C_ID \
                       			\WHERE strftime('%Y-%m-%d',Prices.Date) BETWEEN ? AND ? \ 
                       			\GROUP BY Prices.COMP_ID \
                       			\ORDER BY Prices.Close desc" [toSql fromDate, toSql toDate]  
       commit conn                			
       return (map(map (\x -> fromSql x)) res)       

       
       
-- |Method to print the lowest price close, sorted by price and for all companies
cheapestPriceAll :: IConnection conn => conn -> IO [[String]]
cheapestPriceAll conn  =
    do res <- quickQuery' conn "SELECT \
                       			\MIN(Prices.Close), Prices.Date,Companies.Name \
                       			\FROM Prices \
                       			\INNER JOIN Companies \
                       			\ON Prices.COMP_ID=Companies.C_ID \
                       			\GROUP BY Prices.COMP_ID \
                       			\ORDER BY Prices.Close desc" [] 
       commit conn                			
       return (map(map (\x -> fromSql x)) res)
 
-- |Method to print the lowest price close, sorted by price and for all companies
cheapestPriceAllDateRange :: IConnection conn => conn -> String -> String -> IO [[String]]
cheapestPriceAllDateRange conn fromDate toDate  =
    do res <- quickQuery' conn "SELECT \
                       			\MIN(Prices.Close), Prices.Date,Companies.Name \
                       			\FROM Prices \
                       			\INNER JOIN Companies \
                       			\ON Prices.COMP_ID=Companies.C_ID \
                       			\WHERE strftime('%Y-%m-%d',Prices.Date) BETWEEN ? AND ? \ 
                       			\GROUP BY Prices.COMP_ID \
                       			\ORDER BY Prices.Close desc" [toSql fromDate,toSql toDate] 
       commit conn                			
       return (map(map (\x -> fromSql x)) res)       
       
       
       
-- |Method to print the cheapest price close, sorted by price and for one company
cheapestPrice :: IConnection conn => conn -> Int -> IO [[String]]
cheapestPrice conn id =
    do res <- quickQuery' conn "SELECT \
                       			\MIN(Prices.Close), Prices.Date,Companies.Name \
                       			\FROM Prices \
                       			\INNER JOIN Companies \
                       			\ON Prices.COMP_ID=Companies.C_ID \
                       			\WHERE Companies.C_ID=? \
                       			\ORDER BY Prices.Close desc" [toSql id]
       commit conn
       return (map(map (\x -> fromSql x)) res)
       
-- |Method to print the cheapest price close, sorted by price and for one company
cheapestPriceDateRange :: IConnection conn => conn -> Int -> String -> String -> IO [[String]]
cheapestPriceDateRange conn id fromDate toDate =
    do res <- quickQuery' conn "SELECT \
                       			\MIN(Prices.Close), Prices.Date,Companies.Name \
                       			\FROM Prices \
                       			\INNER JOIN Companies \
                       			\ON Prices.COMP_ID=Companies.C_ID \
                       			\WHERE Companies.C_ID=? AND strftime('%Y-%m-%d',Prices.Date) BETWEEN ? AND ? \
                       			\ORDER BY Prices.Close desc" [toSql id,toSql fromDate, toSql toDate]
       commit conn
       return (map(map (\x -> fromSql x)) res)
       
       
       
-- |Method to print the avg price closes, sorted by price and for all companies
avgPriceAll :: IConnection conn => conn -> IO [[String]]
avgPriceAll conn =
    do res <- quickQuery' conn "SELECT \
                       			\AVG(Prices.Close), Prices.Date,Companies.Name \
                       			\FROM Prices \
                       			\INNER JOIN Companies \
                       			\ON Prices.COMP_ID=Companies.C_ID \
                       			\GROUP BY Prices.COMP_ID \
                       			\ORDER BY Prices.Close desc" []  
       commit conn
       return (map(map (\x -> fromSql x)) res)
       
       
-- |Method to print the avg price closes, sorted by price and for all companies
avgPriceDateRange :: IConnection conn => conn -> String -> String -> IO [[String]]
avgPriceDateRange conn fromDate toDate =
    do res <- quickQuery' conn "SELECT \
                       			\AVG(Prices.Close), Prices.Date,Companies.Name \
                       			\FROM Prices \
                       			\INNER JOIN Companies \
                       			\ON Prices.COMP_ID=Companies.C_ID \
                       			\WHERE strftime('%Y-%m-%d',Prices.Date) BETWEEN ? AND ? \ 
                       			\GROUP BY Prices.COMP_ID \
                       			\ORDER BY Prices.Close desc" [toSql fromDate,toSql toDate]  
       commit conn
       return (map(map (\x -> fromSql x)) res)
       

--Advanced Queries      
       
-- |Method to print the avg price closes, sorted by price and for all companies
estMoneyMade :: IConnection conn => conn -> Int -> IO [[String]]
estMoneyMade conn id  =
    do res <- quickQuery' conn "SELECT \
                       			\Prices.Date,Companies.Name,\
                       			\Prices.Volume * (Prices.Low + Prices.High)/2 AS MoneyMade \
                       			\FROM Prices \
                       			\INNER JOIN Companies \
                       			\ON Prices.COMP_ID=Companies.C_ID \
                       			\WHERE Companies.C_ID=? \
                       			\ORDER BY MoneyMade desc" [toSql id] 
       commit conn
       return (map(map (\x -> fromSql x)) res)
       
       
       
-- |Method to print the average prices for each month.
avgPriceByMonth :: IConnection conn => conn -> Int -> IO [[String]]
avgPriceByMonth conn id  =
    do res <- quickQuery' conn "SELECT \
                       			\strftime('%m',Date) AS Month,\
                       			\Companies.Name,AVG(Prices.Close) AS AverageClose \
                       			\FROM Prices \
                       			\INNER JOIN Companies \
                       			\ON Prices.COMP_ID=Companies.C_ID \
                       			\WHERE Companies.C_ID=? \
                       			\GROUP BY Month" [toSql id]  
       commit conn
       return (map(map (\x -> fromSql x)) res) 
       
       
-- |Method to print the average prices for each year, for all companies ordered by AverageClose desc.
avgPriceByYear :: IConnection conn => conn -> IO [[String]]
avgPriceByYear conn  =
    do res <- quickQuery' conn "SELECT \
                       			\strftime('%Y',Date) AS Year,\
                       			\Companies.Name,AVG(Prices.Close) AS AverageClose \
                       			\FROM Prices \
                       			\INNER JOIN Companies \
                       			\ON Prices.COMP_ID=Companies.C_ID \
                       			\GROUP BY Year,Companies.C_ID \
                       			\ORDER BY AverageClose desc" [] 
       commit conn
       return (map(map (\x -> fromSql x)) res)  

    
       
--printing methods


-- |Method to print error msg for companies
printString :: IConnection conn => conn -> IO(String)
printString conn = do 
					  ch <- getCompanies conn
					  let str = buildString(ch)
					  return (str)
					  
-- |Method to build a string with all the current companies in the db. To show the prompt					
buildString :: [Company]-> String
buildString [] =""
buildString (c:xs)= (show $ (identifier c))++": "++(name c) ++" " ++buildString(xs)
       
-- |Method to display prices by company
printpbc :: IConnection conn => conn -> Int -> IO ()
printpbc conn id = do pc <- priceByComp conn id
                      mapM_ print pc
                      
-- |Method to display prices by company
printpbcdr :: IConnection conn => conn -> Int -> String -> String -> IO ()
printpbcdr conn id fromDate toDate =
    do p <- priceByCompDateRange conn id fromDate toDate
       mapM_ print p
                      
-- |Method to display cheapest prices for a company
printcheapest :: IConnection conn => conn -> Int -> IO ()
printcheapest conn id = do ch <- cheapestPrice conn id
                           mapM_ print ch
                           
-- |Method to display prices by company
printcheapestdr :: IConnection conn => conn -> Int -> String -> String -> IO ()
printcheapestdr conn id fromDate toDate =
    do cpr <- cheapestPriceDateRange conn id fromDate toDate
       mapM_ print cpr
 
-- |Method to display highest prices for one company
printhighest :: IConnection conn => conn -> Int -> IO ()
printhighest conn id = do hp <- highestPrice conn id
                          mapM_ print hp

-- |Method to display prices by company
printhighestdr :: IConnection conn => conn -> Int -> String -> String -> IO ()
printhighestdr conn id fromDate toDate =
    do hpr <- highestPriceDateRange conn id fromDate toDate
       mapM_ print hpr
                                 
                       
-- |Method to display highest prices for all companies
printhighestpriceall :: IConnection conn => conn -> IO ()
printhighestpriceall conn = do hp <- highestPriceAll conn
                               mapM_ print hp
   
-- |Method to display prices by company
printhighestalldr :: IConnection conn => conn -> String -> String -> IO ()
printhighestalldr conn fromDate toDate =
    do hpar <- highestPriceAllDateRange conn fromDate toDate
       mapM_ print hpar                               
                               
                               
                             
-- |Method to display lowest prices for all companies
printcheapestpriceall :: IConnection conn => conn -> IO ()
printcheapestpriceall conn = do cp <- cheapestPriceAll conn
                                mapM_ print cp
 
                              
-- |Method to display prices by company
printcheapestpricealldr :: IConnection conn => conn -> String -> String -> IO ()
printcheapestpricealldr conn fromDate toDate =
    do cpar <- cheapestPriceAllDateRange conn fromDate toDate
       mapM_ print cpar
                                      
                              
                              
-- |Method to display avg prices for all companies
printavgall :: IConnection conn => conn -> IO ()
printavgall conn = do avg <- avgPriceAll conn
                      mapM_ print avg
 
     
-- |Method to display prices by company
printavgpricedr :: IConnection conn => conn -> String -> String -> IO ()
printavgpricedr conn fromDate toDate =
    do apr <- avgPriceDateRange conn fromDate toDate
       mapM_ print apr                      
                      
                      
-- |Method to display the moneymade on each day for one company
printestmoneymade :: IConnection conn => conn -> Int -> IO ()
printestmoneymade conn id = do mm <- estMoneyMade conn id
                               mapM_ print mm
                               
-- |Method to the average close for one company, grouped by month
printavgpricebymonth :: IConnection conn => conn -> Int -> IO ()
printavgpricebymonth conn id = do apbm <- avgPriceByMonth conn id
                                  mapM_ print apbm  

-- |Method to the average close for all companies, grouped by year
printavgpricebyyear :: IConnection conn => conn -> IO ()
printavgpricebyyear conn = do apby <- avgPriceByYear conn
                              mapM_ print apby  


