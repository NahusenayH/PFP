import "lib/github.com/diku-dk/sorts/radix_sort" 

def segscan [n] 't (op: t -> t -> t) (ne: t) (arr: [n](t, bool)): *[n]t =
    let op' = \(v1,f1) (v2,f2) -> (if f2 then v2 else op v1 v2 , f1 || f2)
    in map(\(t,_)->t) (scan  op' (ne,false) arr)
    
def snd (_,v2) = v2 
def fst (k,_) = k

def segreduce [n] 't (op: t -> t -> t) (ne: t) (arr: [n](t, bool)): *[]t =
  let scan_arr = segscan op ne arr
  let segment_ends = rotate 1 (map snd arr)
  let segment_end_offsets = segment_ends |> map i64.bool |> scan (+) 0
  let num_segments = if n > 1 then segment_end_offsets[n-1] else 0i64
  let scratch = replicate num_segments ne
  let index i f = if f then if i > 0 then i - 1 else 0 else -1
  in scatter scratch (map2 index segment_end_offsets segment_ends) scan_arr

def hist' 'a [n] (op: a -> a -> a) (ne: a) (k: i64) (is: [n]i32) (as: [n]a) : [k]a =
  let get_bit (i: i32) (x: i32): i32 = (x >> i) & 1
  let sorted = radix_sort_by_key (\(i, _) -> i) 32 get_bit (zip is as)
  let sorted_as = (map snd sorted)
  let flags = map2 (\i x -> if i == 0 then true else x != fst sorted[i-1]) (iota n) (map fst sorted)
  let reduced = segreduce op ne (zip sorted_as flags)
  in  take k reduced

--def main [n] (k:i64) (is: [n]i32) (as:[n]i32): [k]i32 = hist' (+) 0 k is as

def main [n] (k:i64) (is: [n]i64) (as:[n]i64): [k]i64 = hist (+) 0 k is as

-- random input { 100i64 [100]i32 [100]i32} auto output
-- random input { 1000i64 [1000]i32 [1000]i32} auto output
-- random input { 10000i64 [10000]i32 [10000]i32} auto output
-- random input { 100000i64 [100000]i32 [100000]i32} auto output



-- test process.
-- ==

-- random input { 1000i64 [1000]i64 [1000]i64} auto output
-- random input { 10000i64 [10000]i64 [10000]i64} auto output
-- input { 100000000000i64 [100000000000]i64 [100000000000]i64}