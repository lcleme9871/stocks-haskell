-- |Module that holds type constructors and methods for execption handling


module CSVDATA where

import Control.Monad.Error
import Database.HDBC
import Data.Maybe
import Data.Char hiding (isNumber)


data Stock = Stock { date :: String  
              , open :: Double
              , high :: Double 
              , low :: Double  
              , close :: Double  
              , volume :: Int
              , adj :: Double
              , key :: Int
             } deriving (Eq,Show) 
             
data Company = Company { identifier::Int
						,name :: String
						,url :: String
						,link :: String
						} deriving(Eq, Show)
						



data InputError = EmptyString  -- Entered string was empty.
          | StringTooLong Int   -- A string is longer than 3 characters.
          | WrongArg            --An incorrect argument has been entered at the terminal
          | NotInt String   -- the string doesn't represent an int
          | OtherError String   -- Any other error, apart from the above
          | ConnectionError String   -- Error connecting
          | AddPrompt                --Error msg at the add prompt
          




{- | Instance of the error type

We make inputError an instance of the Error class
This is because it can be thrown as an exception
-}
instance Error InputError where
  noMsg    = OtherError "Unexpected Error"
  strMsg s = OtherError s
  
{- | Converts InputError to a range of messages
-}
instance Show InputError where
  show EmptyString = "The string was empty!"
  show (StringTooLong len) =
      "The length of the string (" ++ (show len) ++ ") is bigger than 3!"
  show (NotInt s) =
      "Input is not an Int (" ++ (show s) ++ ") Try again!"    
  show (OtherError msg) = msg
  show (ConnectionError msg) = "Error connecting: " ++msg
  show WrongArg =
  	"Usage: Incorrect argument. Try one of these: [args]\n\
  	\\n\
  	\addcomp          Prints list of companies to add to db\n\
  	\updatecomp id    updates a .csv url by downloading a new one\n\
  	\addstock id      parses the company file in the db, stores the results\n\
  	\showcomp         Displays all companies\n\
  	\showstock id     Displays all stock\n\
  	\deletecompany id Deletes a given company and all it's related stock\n\
  	\pricebycomp id   Displays prices by a given company\n\
  	\cheapestprice id Displays the cheapest price for a given company\n\
  	\highestprice id  Displays the highest price for a given company\n\
  	\highestpriceall  Displays the highest price for all companies\n\
  	\lowestpriceall   Displays the lowest price for all companies\n\
  	\avgall           Displays the avg price for all companies\n\
  	\estmoneymade     Calculates the estimiated money made for each company\n\
  	\avgpricebymonth  Calcuates the avg price, grouped by month for each company\n\
  	\avgpricebyyear   Calculates the avg price by year, group by year and company"
  	
  show AddPrompt =
  	"Enter the id next to a company to add to the db:]\n\
  	\\n\
  	\1:   Google\n\
  	\2:   Facebook\n\
    \3:   Yahoo\n\
  	\4    Twitter\n"
  	
{- | InputMonad
For our monad type constructor, we use Either InputError
which represents failure using Left InputError
or a successful result of type a using Right a.
Reference : https://hackage.haskell.org/package/mtl-2.2.1/docs/Control-Monad-Error.html
-}
type InputMonad = Either InputError






{- | Converts an input id into an int, returns error otherwise
Wraps input calculation to catch the errors.
Returns either length of the string or an error.
-}
convert :: String -> InputMonad Int
convert s = (check s) `catchError` Left

{- | Method to test whether the input is valid
Attempts to calculate length and throws an error if the provided string is
empty or longer than 3 characters. 3 was choosen because their will never be more than 999 companies in the database
The processing is done in Either monad.
-}
check :: String -> InputMonad Int
check [] = throwError EmptyString
check s | (length s) > 3 = throwError (StringTooLong (length s))
        | ((isN s))==False = throwError (NotInt s)
                        | otherwise = return (read s ::Int)

{-| Method check whether result is correct, throws error otherwise -}
checkResult :: InputMonad Int -> Int
checkResult (Right num) = num
checkResult (Left e) = error $ "Length error" ++ (show e)

{-| Method check whether an input is a number or not -}
isN :: [Char] -> Bool 
isN [] = True 
isN (x:xs) = isDigit x && isN xs 


  