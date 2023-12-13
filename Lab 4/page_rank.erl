%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% This implements a page rank algorithm using map-reduce
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-module(page_rank).
-compile([export_all,nowarn_export_all]).

%% Use map_reduce to count word occurrences

map(Url,ok) ->
    [{Url,Body}] = dets:lookup(web,Url),
    Urls = crawl:find_urls(Url,Body),
    [{U,1} || U <- Urls].

reduce(Url,Ns) ->
    [{Url,lists:sum(Ns)}].

page_rank() ->
    {ok,web} = dets:open_file(web,[{file,"web.dat"}]),
    Urls = dets:foldl(fun({K,_},Keys)->[K|Keys] end,[],web),
    map_reduce:map_reduce_seq(fun map/2, fun reduce/2, 
			      [{Url,ok} || Url <- Urls]).

page_rank_par() ->
    dets:open_file(web,[{file,"web.dat"}]),
    Urls = dets:foldl(fun({K,_},Keys)->[K|Keys] end,[],web),
    map_reduce:map_reduce_par(fun map/2, 32, fun reduce/2, 32, 
			      [{Url,ok} || Url <- Urls]).





%% -------------

page_rank_parDistri() ->
    {ok,web} = dets:open_file(web,[{file,"web.dat"}]),
    Urls = dets:foldl(fun({K,_},Keys)->[K|Keys] end,[],web),
    map_reduce:map_reduce_parDistri(fun map/2, 32, fun reduce/2, 32, 
			      [{Url,ok} || Url <- Urls]).


%% -------------


page_rank_parPool() ->
    {ok,web} = dets:open_file(web,[{file,"web.dat"}]),
    Urls = dets:foldl(fun({K,_},Keys)->[K|Keys] end,[],web),
    io:format("First"),
    map_reduce:map_reduce_parPool(fun map/2, 32, fun reduce/2, 32, 
			      [{Url,ok} || Url <- Urls]).

%% -------------
page_rank_parFaulty() ->
    {ok,web} = dets:open_file(web,[{file,"web.dat"}]),
    Urls = dets:foldl(fun({K,_},Keys)->[K|Keys] end,[],web),
    io:format("First"),
    map_reduce:map_reduce_parFaulty(fun map/2, 32, fun reduce/2, 32, 
			      [{Url,ok} || Url <- Urls]).








benchmarksSeq() ->
    timer:tc(?MODULE, page_rank, []).

benchmarksPar1() ->
    timer:tc(?MODULE, page_rank_par, []).

benchmarksParDist() ->
    timer:tc(?MODULE, page_rank_parDistri, []).

benchmarksParPool() ->
    timer:tc(?MODULE, page_rank_parPool, []).

benchmarksParFaulty() ->
    timer:tc(?MODULE, page_rank_parFaulty, []).
