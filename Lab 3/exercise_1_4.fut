-- ==
-- input @ two_100_i32s
-- input @ two_1000_i32s
-- input @ two_10000_i32s
-- input @ two_100000_i32s
-- input @ two_1000000_i32s
-- input @ two_5000000_i32s
-- input @ two_10000000_i32s

def MININT : i32 = -10000000

def mx (m1:i32,i1:i64) (m2:i32,i2:i64) : (i32,i64) =
  if m1 > m2 then (m1,i1) else (m2,i2)

def process_idx [n] ( xs : [n]i32 ) (ys : [n] i32) : (i32,i64) =
    reduce mx (MININT, -1) (zip (map i32 . abs ( map2 (-) xs ys )) (iota n))

def main ( xs : [] i32 ) ( ys : [] i32 ) = process_idx xs ys