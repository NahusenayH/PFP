import Sudoku
import Control.Exception
import System.Environment
import Control.Parallel.Strategies hiding (parMap)
import Data.Maybe
import Criterion.Main
-- -- <<main
-- main :: IO ()
-- main = do
--   [f] <- getArgs
--   file <- readFile f

--   let puzzles   = lines file
--       solutions = runEval (parMap solve puzzles)

--   evaluate (length puzzles)
--   print (length (filter isJust solutions))
-- -- >>


--- Assignment 3

parMap :: (a -> b) -> [a] -> Eval [b]
parMap f [] = return []
parMap f (a:as) = do
   b <- rpar (f a)
   bs <- parMap f as
   return (b:bs)

helperr xs = runEval (parMap solve xs)

bufferPar :: NFData b => (a->b) -> [a] -> [b]
bufferPar f xs = withStrategy (parBuffer 100 rdeepseq) (map f xs)

listPar :: NFData b => (a->b) -> [a] -> [b]
listPar f xs = withStrategy (parList rdeepseq) (map f xs)

dynParListChunks:: NFData b => (a->b) -> [a] -> [b]
dynParListChunks _ [] = []
dynParListChunks f (xs) = map f xs `using` (parListChunk ((length xs) `div` 5) rdeepseq)


--- 3 c)
bufferParchunks :: NFData b => (a->b) -> [a] -> [b]
bufferParchunks f xs = withStrategy (parBuffer 100 rdeepseq) (concat (map (map f) chunkss))
  where chunkss = chunksOf ((length xs) `div` 4) xs

chunksOf :: Int -> [a] -> [[a]]
chunksOf n [] = []
chunksOf n xs = fst splitted : chunksOf n (snd splitted)
  where splitted = splitAt n xs



--- main
main :: IO ()
main = do
  file <- readFile "sudokumid.txt" 

  let puzzles   = lines file
      solutions = bufferParchunks solve puzzles

  evaluate (length puzzles)
  print (length (filter isJust solutions))

  defaultMain
        [
         bench "parMap" (nf (helperr) puzzles),
         bench "parBuffer" (nf (bufferPar solve) puzzles),
         bench "parList" (nf (listPar solve) puzzles)

         ]



