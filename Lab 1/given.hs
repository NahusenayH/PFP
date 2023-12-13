{-# LANGUAGE NumericUnderscores #-}

import Data.List
import System.Random
import Criterion.Main
import Control.Parallel
import Control.Parallel.Strategies(rpar, rseq, runEval, using, parMap, rparWith, rdeepseq, NFData, parListChunk)

import Sudoku(solve)

import Control.Monad.Par ( NFData, spawn, spawnP, get, runPar, Par )


-- code borrowed from the Stanford Course 240h (Functional Systems in Haskell)
-- I suspect it comes from Bryan O'Sullivan, author of Criterion






data T a = T !a !Int

mean :: (RealFrac a) => [a] -> a
mean = fini . foldl' go (T 0 0)
  where
    fini (T a _) = a
    go (T m n) x = T m' n'
      where m' = m + (x - m) / fromIntegral n'
            n' = n + 1

resamples :: Int -> [a] -> [[a]]
resamples k xs =
    take (length xs - k) $
    zipWith (++) (inits xs) (map (drop k) (tails xs))


crud = zipWith (\x a -> sin (x / 300)**2 + a) [0..]


main = mainAssignment2


--- Assignment 1

jackknife :: ([a] -> b) -> [a] -> [b]
jackknife f = map f . resamples 500

--- 1 a)
pmap:: (a->b) -> [a] -> [b]
pmap _ [] = []
pmap f (x:xs) =  par fx (pseq fxs (fx:fxs))
   where fx = f x
         fxs = pmap f xs

jackknife1 :: ([a] -> b) -> [a] -> [b]
jackknife1 f = pmap f . resamples 500


--- 1 b)
rpmap:: (a->b) -> [a] -> [b]
rpmap _ [] = []
rpmap f (x:xs) = runEval $ do
                   fx <- rpar $ f x
                   fxs <- rseq $ rpmap f xs
                   return (fx:fxs)

jackknife2 :: ([a] -> b) -> [a] -> [b]
jackknife2 f = rpmap f . resamples 500



jackknife22 :: ([a] -> b) -> [a] -> [b]
jackknife22 f = parMap rpar f . resamples 500


--- 1 c)
rStratpmap:: NFData b => (a->b) -> [a] -> [b]
rStratpmap _ [] = []
rStratpmap f (xs) = map f xs `using` (parListChunk ((length xs) `div` 5) rdeepseq)


jackknife3 :: NFData b => ([a] -> b) -> [a] -> [b]
jackknife3 f = rStratpmap f . resamples 500


--- 1 d)

rParMmap :: NFData b => (a -> b) -> [a] -> [b]
rParMmap f as = runPar $ do
 ibs <- mapM (spawn . return . f) as
 mapM get ibs


jackknife4 :: NFData b => ([a] -> b) -> [a] -> [b]
jackknife4 f = rParMmap f . resamples 500




mainAssignment1 = do
  let (xs,ys) = splitAt 1500  (take 6000
                               (randoms (mkStdGen 211570155)) :: [Float] )
  -- handy (later) to give same input different parallel functions

  let rs = crud xs ++ ys
  putStrLn $ "sample mean:    " ++ show (mean rs)

  let j = jackknife mean rs :: [Float]
  putStrLn $ "jack mean min:  " ++ show (minimum j)
  putStrLn $ "jack mean max:  " ++ show (maximum j)
  defaultMain
        [
         bench "jackknifeSequential" (nf (jackknife  mean) rs),
         bench "jackknifeA" (nf (jackknife1  mean) rs),
         bench "jackknifeB"  (nf (jackknife2  mean) rs),
         bench "jackknifeParMap"  (nf (jackknife22  mean) rs),
         bench "jackknifeStrat"  (nf (jackknife3  mean) rs),
         bench "jackknifeParM"  (nf (jackknife4  mean) rs)
         ]



------------------------------ Assignment 2


-- divideAndConquer :: (a -> Bool)  -- should we still divide?
--         -> (a -> [a])   -- divide
--         -> (a -> b)     -- solve
--         -> ([b] -> b)   -- conquer
--         -> a            -- the input
--         -> b            -- the output


seqDivideAndConquer :: NFData b => (a -> Bool) -> (a -> b) -> (a -> [a]) -> ([b] -> b) -> a -> b
seqDivideAndConquer boolcheck solve divide combine input =
  if boolcheck input
    then combine $ map (seqDivideAndConquer boolcheck solve divide combine) (divide input)
    else solve input

parDivideAndConquer :: NFData b => (a -> Bool) -> (a -> b) -> (a -> [a]) -> ([b] -> b) -> a -> b
parDivideAndConquer boolcheck solve divide combine input =
  if boolcheck input
    then combine $ rParMmap (parDivideAndConquer boolcheck solve divide combine) (divide input)
    else solve input

------------------- Mergesort
sortSplitCheck :: [a] -> Bool
sortSplitCheck xs = if length xs > 10000 then True else False

divideSort :: [a] -> [[a]]
divideSort xs = [v, h]
  where (v, h) = splitAt ((length xs) `div` 2) xs

sortMerge :: Ord a => [[a]] -> [a]
sortMerge [] = []
sortMerge [[], ys] = ys
sortMerge [xs, []] = xs
sortMerge [(x:xs), (y:ys)] = if x < y then x : sortMerge [xs, (y:ys)] else y : sortMerge [(x:xs), ys]

sorting :: Ord a => [a] -> [a]
sorting [] = []
sorting (l:[]) = [l]
sorting ls = sortMerge [sorting v, sorting h]
  where [v, h] = divideSort ls

----------- Factorial
divideFact :: [a] -> [[a]]
divideFact xs = [v, h]
  where (v, h) = splitAt ((length xs) `div` 2) xs

solveFact :: Num a => [a] -> a
solveFact l = case l of
  [] -> 1
  (x:xs) -> x * solveFact xs

combinefact :: Num a => [a] -> a
combinefact [m, n] = m * n

factSplitCheck :: [a] -> Bool
factSplitCheck xs = if length xs > 10000 then True else False


----- Main for assignment 2
mainAssignment2 = do
  let xs =   (take 1_000_000
                               (randoms (mkStdGen 211570155)) :: [Float] )
  -- handy (later) to give same input different parallel functions
  let listus = seqDivideAndConquer sortSplitCheck sorting divideSort sortMerge xs
  print ""
  --Checking that they compute the right result
  print $ seqDivideAndConquer sortSplitCheck sorting divideSort sortMerge ([9,8,7,6,5,4,3,2,1]::[Float])
  print $ parDivideAndConquer sortSplitCheck sorting divideSort sortMerge ([9,8,7,6,5,4,3,2,1]::[Float])
  print $ seqDivideAndConquer factSplitCheck solveFact divideFact combinefact ([1..10]::[Integer])
  print $ parDivideAndConquer factSplitCheck solveFact divideFact combinefact ([1..10]::[Integer])
 
  defaultMain
       [
         bench "sequentialSort" (nf (seqDivideAndConquer sortSplitCheck sorting divideSort sortMerge ) xs ),
         bench "parallellSort" (nf (parDivideAndConquer sortSplitCheck sorting divideSort sortMerge ) xs ),
         bench "sequentialFact" (nf (seqDivideAndConquer factSplitCheck solveFact divideFact combinefact ) ([1..1000000]::[Int]) ),
         bench "parallellFact" (nf (parDivideAndConquer factSplitCheck solveFact divideFact combinefact ) ([1..1000000]::[Int]) )
        ]