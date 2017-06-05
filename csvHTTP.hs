-- |Module that performs http requests as well as parsing.
module CSVHTTP where

import CSVDATA
import Text.Parsec.String
import Text.Parsec hiding (Empty)
import Network.HTTP hiding (close)
import Network.URI
import Data.Maybe
import Data.Either
import Data.Word
import Data.List hiding (endBy,sepBy)
import qualified Data.ByteString.Lazy as L
import Control.Exception hiding (try)
import System.Environment


     

             
-- |This function will download a given url and return its content as a String
-- This uses simpleHTTP which is not as fast as simpleHttp
-- This function also uses the Either Monad for exceptions
downloadURL2 :: String -> IO (Either String String)
downloadURL2 url =
    do resp <- simpleHTTP request
       case resp of
         Left x -> return $ Left ("Error connecting: " ++ show x)
         Right r -> 
             case rspCode r of
               (2,_,_) -> return $ Right (rspBody r)
               (3,_,_) -> -- A HTTP redirect
                 case findHeader HdrLocation r of
                   Nothing -> return $ Left (show r)
                   Just url -> downloadURL2 url
               _ -> return $ Left (show r)
    where request = Request {rqURI = uri,
                             rqMethod = GET,
                             rqHeaders = [],
                             rqBody = ""}
          uri = fromJust $ parseURI url    
             
             
       
--CSV Parsing---------------- 
--Reference --Real World Haskell
--by Bryan O'Sullivan, Don Stewart, and John Goerzen
--Chapter 16: http://book.realworldhaskell.org/read/using-parsec.html

csvFile = endBy line eol
line = sepBy cell (char ',')
cell = quotedCell <|> many (noneOf ",\n\r")

quotedCell = 
    do char '"'
       content <- many quotedChar
       char '"' <?> "quote at end of cell"
       return content

quotedChar =
        noneOf "\""
    <|> try (string "\"\"" >> return '"')

eol =   try (string "\n\r")
    <|> try (string "\r\n")
    <|> string "\n"
    <|> string "\r"
    <?> "end of line"
    
--CSV Parsing---------------- 
    
-- |Basic method for extracting all urls of a given text (String)
parseURLs :: String               -- ^ String representation of html file
          -> [String]
parseURLs [] = []
parseURLs ('h':'t':'t':'p':':':'/':'/':xs) = ("http://" ++ url) : (parseURLs rest)
     where (url, rest) = break space xs
           space c = elem c [' ','\t','\n','\r','\"','\'',')',';','<']
parseURLs (_:xs) = parseURLs xs


-- |This function returns a string which is the url to a .csv file
getLink :: [String] -- ^ String representation text from html file
		-> String
getLink [] = ""
getLink(x:xs)= if(isInfixOf ".csv" x) 
			  then getLink(xs) ++ x 
			  else getLink(xs) ++ ""

convertToStock :: [String] -- ^ Takes a list of Strings and returns a Stock data type
     -> Stock 
convertToStock (d:o:h:l:c:v:a:xs) =Stock {date=(d), open= read o:: Double, high=read h:: Double,low=read l:: Double,close=read c:: Double,volume=read v:: Int,adj=read a:: Double,key=read (head xs):: Int}

--helper methods--

-- |Parses the csv file and returns the file as a matrix wrapped by a monad
parseCSV :: String -> Either ParseError [[String]]
parseCSV input = parse csvFile "(unknown)" input

-- |Returns the file without the monad
justTheList ::Either ParseError [[String]] -> [[String]]
justTheList result = either (const []) id result


-- |Removes the table header from the file, we don't need this.
removeHeader :: [[String]] -> [[String]]
removeHeader list = tail(list) 

-- |appends a key to a list to represent a company key. This will be needed as a FK in the DB
addKey :: [[String]] -> [String] -> [[String]]
addKey [] _ = []
addKey(x:xs) key = (x ++ key) : addKey xs key 