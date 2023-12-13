%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% This is a very simple implementation of map-reduce, in both 
%% sequential and parallel versions.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-module(map_reduce).
-compile([export_all,nowarn_export_all]).

%% We begin with a simple sequential implementation, just to define
%% the semantics of map-reduce. 

%% The input is a collection of key-value pairs. The map function maps
%% each key value pair to a list of key-value pairs. The reduce
%% function is then applied to each key and list of corresponding
%% values, and generates in turn a list of key-value pairs. These are
%% the result.

map_reduce_seq(Map,Reduce,Input) ->
    Mapped = [{K2,V2}
	      || {K,V} <- Input,
		 {K2,V2} <- Map(K,V)],
    io:format("Map phase complete\n"),
    reduce_seq(Reduce,Mapped).

reduce_seq(Reduce,KVs) ->
    [KV || {K,Vs} <- group(lists:sort(KVs)),
	   KV <- Reduce(K,Vs)].

group([]) ->
    [];
group([{K,V}|Rest]) ->
    group(K,[V],Rest).

group(K,Vs,[{K,V}|Rest]) ->
    group(K,[V|Vs],Rest);
group(K,Vs,Rest) ->
    [{K,lists:reverse(Vs)}|group(Rest)].

map_reduce_par(Map,M,Reduce,R,Input) ->
    Parent = self(),
    Splits = split_into(M,Input),
    Mappers = 
	[spawn_mapper(Parent,Map,R,Split)
	 || Split <- Splits],
    Mappeds = 
	[receive {Pid,L} -> L end || Pid <- Mappers],
    io:format("Map phase complete\n"),
    Reducers = 
	[spawn_reducer(Parent,Reduce,I,Mappeds) 
	 || I <- lists:seq(0,R-1)],
    Reduceds = 
	[receive {Pid,L} -> L end || Pid <- Reducers],
    io:format("Reduce phase complete\n"),
    lists:sort(lists:flatten(Reduceds)).

spawn_mapper(Parent,Map,R,Split) ->
    spawn_link(fun() ->
			Mapped = [{erlang:phash2(K2,R),{K2,V2}}
				  || {K,V} <- Split,
				     {K2,V2} <- Map(K,V)],
                        io:format("."),
			Parent ! {self(),group(lists:sort(Mapped))}
		end).

split_into(N,L) ->
    split_into(N,L,length(L)).

split_into(1,L,_) ->
    [L];
split_into(N,L,Len) ->
    {Pre,Suf} = lists:split(Len div N,L),
    [Pre|split_into(N-1,Suf,Len-(Len div N))].

spawn_reducer(Parent,Reduce,I,Mappeds) ->
    Inputs = [KV
	      || Mapped <- Mappeds,
		 {J,KVs} <- Mapped,
		 I==J,
		 KV <- KVs],
    spawn_link(fun() -> Result = reduce_seq(Reduce,Inputs),
                        io:format("."),
                        Parent ! {self(),Result} end).





%% ----------------------










map_reduce_parDistri(Map,M,Reduce,R,Input) ->
    Parent = self(),
    Nds = nodes() ++ [node()],
    LenNds = length(Nds),
    Splits = split_into(M,Input),
    CombSplits = lists:zip(Nds, split_into(LenNds, Splits)),
    Mappers = 
	[spawn_mapperDistri(Parent,Map,R,Split, Nd)
	 || {Nd, Spls} <- CombSplits, Split <- Spls],
    Mappeds = 
	[receive {Pid,L} -> L end || Pid <- Mappers],
    io:format("Map phase complete\n"),
    Reducers = 
	[spawn_reducerDistri(Parent,Reduce,I,Mappeds,Nd) 
	 || {Nd, Ixs} <- (lists:zip(Nds, split_into(LenNds, lists:seq(0,R-1)))), I <- Ixs],
    Reduceds = 
	[receive {Pid,L} -> L end || Pid <- Reducers],
    io:format("Reduce phase complete\n"),
    lists:sort(lists:flatten(Reduceds)).



spawn_mapperDistri(Parent,Map,R,Split,Nd) ->
    spawn_link(Nd, fun() ->
            {ok,web} = dets:open_file(web,[{file,"web.dat"}]),
			Mapped = [{erlang:phash2(K2,R),{K2,V2}}
				  || {K,V} <- Split,
				     {K2,V2} <- Map(K,V)],
                        io:format("."),
			Parent ! {self(),group(lists:sort(Mapped))}
		end).


spawn_reducerDistri(Parent,Reduce,I,Mappeds,Nd) ->
    Inputs = [KV
	      || Mapped <- Mappeds,
		 {J,KVs} <- Mapped,
		 I==J,
		 KV <- KVs],
    spawn_link(Nd, fun() -> Result = reduce_seq(Reduce,Inputs),
                        io:format("."),
                        Parent ! {self(),Result} end).




%% --------------



arbetare(Queen) ->
    Queen ! {petition, self()},

    receive 
	{toil, Pollen} -> 
	    Queen ! {finished, Pollen()},
	    arbetare(Queen);

	kill -> exit(killed)
    end.


%% C is the count of how many processes we want per node
spawn_hive(C) ->
    Queen = self(),
    [spawn(Nod, fun() -> arbetare(Queen) end) || _ <- lists:seq(1,C), Nod <- nodes() ++ [node()]].


%% Pollen = the functions that we want to execute with the hive of worker-bees
worker_pool(Pollen) ->
    Id = self(),

    spawn(fun() -> 
		  Hive = spawn_hive(4),
		  working(Pollen, Hive, length(Pollen), [], Id)
	  end),

    receive
	    {Honey} -> Honey
    end.


working(_,Hive,0,Honey, Id) -> 
    [Arbetare ! kill || Arbetare <- Hive],
    Id ! {Honey};

%% The worker-bees in the hive take the pollen=functions and make honey=result
working(Pollen,Hive,Tracker,Honey, Id) ->
    receive
	{petition, Arbetare} -> 
	    RemainingPollen = case Pollen of
			    [P|Ps] -> Arbetare ! {toil, P},
                Ps;
			    [] -> Arbetare ! kill, []		       
			end,
            working(RemainingPollen, Hive, Tracker, Honey, Id);

	{finished, H} -> 
	    working(Pollen, Hive, Tracker-1, [H|Honey], Id)				 
    end.


map_reduce_parPool(Map,M,Reduce,R,Input) ->
    Splits = split_into(M, Input),
    io:format(" split"),
    Mappers = [ fun() ->
          {ok,web} = dets:open_file(web,[{file,"web.dat"}]),
          Mapped = [{erlang:phash2(K2,R),{K2,V2}}
                || {K,V} <- Split,
               {K2,V2} <- Map(K,V)],
          io:format("."),
          group(lists:sort(Mapped))
      end || Split <- Splits ],
    io:format(" Mappers"),
    Mappeds = worker_pool(Mappers),
    io:format(" Mappeds"),
    Reducers = [reducerPool(Reduce, I, Mappeds) || I <- lists:seq(0, R-1)],
    io:format(" Reducers"),
    Reduceds = worker_pool(Reducers),
    io:format(" reduceds~n"),
    lists:sort(lists:flatten(Reduceds)).

reducerPool(Reduce,I,Mappeds) ->
    Inputs = [KV || Mapped <- Mappeds,
		 {J,KVs} <- Mapped,
		 I==J,
		 KV <- KVs],
    fun() -> 
	    reduce_seq(Reduce, Inputs)
    end.


%% --------------

%% The worker-bees in the hive take the pollen=functions and make honey=result
workingFaulty(_, Hive, 0, Honey, Id, _, _) -> 
    [Arbetare ! kill || Arbetare <- Hive],
    Id ! {Honey};


workingFaulty(Pollen, Hive, Tracker, Honey, Id, Restless, Toiling) ->
    receive

    {petition, Arbetare} -> 
	    case Pollen of
		[P|Ps] -> 
		    Arbetare ! {toil, P}, 
		    workingFaulty(Ps, Hive, Tracker, Honey, Id, [Arb || Arb <- Restless, Arb /= Arbetare], [{Arbetare, P}|Toiling]);
		[] -> 
		    workingFaulty([], Hive, Tracker, Honey, Id, [Arbetare|Restless], Toiling)
	    end;

	{finished, H, Arbetare} -> 
	    workingFaulty(Pollen, Hive, Tracker-1, [H|Honey], Id, [Arbetare|Restless], 
        [{Arb, Poll} || {Arb, Poll} <- Toiling, Arb /= Arbetare]);

	{'EXIT', Arbetare, _} ->
	    NewHive = [Arb || Arb <- Hive, Arb /= Arbetare],
        io:format("Node is down!"),
	    
	    case [Poll || {Arb, Poll} <- Toiling, Arb == Arbetare] of
            [] ->
                NewRestless = [Arb || Arb <- Restless, Arb /= Arbetare],
                workingFaulty(Pollen, NewHive, Tracker, Honey, Id, NewRestless, Toiling);
    
            SavedPollen ->
                [RL ! safetyCheck || RL <- Restless],
                workingFaulty(SavedPollen ++ Pollen, NewHive, Tracker, Honey, Id, [], 
                [{Arb, Poll} || {Arb, Poll} <- Toiling, Arb /= Arbetare])
            end
	
    end.

arbetareFaulty(Queen) ->
    Queen ! {petition, self()},

    receive
	  {toil, Pollen} -> 
	    Queen ! {finished, Pollen(), self()},
	    arbetareFaulty(Queen);

	  kill -> exit(killed);

      safetyCheck -> arbetareFaulty(Queen)
    end.


%% C is the count of how many processes we want per node
spawn_hiveFaulty(C) ->
    Queen = self(),
    [spawn_link(Nod, fun() -> arbetareFaulty(Queen) end) || _ <- lists:seq(1,C), Nod <- nodes() ++ [node()]].


%% Pollen = the functions that we want to execute with the hive of worker-bees
worker_poolFaulty(Pollen) ->
    Id = self(),

    spawn(fun() -> 
        process_flag(trap_exit, true), %% So that we can catch the terminations
		Hive = spawn_hiveFaulty(4),
		workingFaulty(Pollen, Hive, length(Pollen), [], Id, [], [])
	  end),

    receive
	    {Honey} -> Honey
    end.


map_reduce_parFaulty(Map,M,Reduce,R,Input) ->
    Splits = split_into(M, Input),
    io:format(" split"),
    Mappers = [ fun() ->
          {ok,web} = dets:open_file(web,[{file,"web.dat"}]),
          Mapped = [{erlang:phash2(K2,R),{K2,V2}}
                || {K,V} <- Split,
               {K2,V2} <- Map(K,V)],
          io:format("."),
          group(lists:sort(Mapped))
      end || Split <- Splits ],
    io:format(" Mappers"),
    Mappeds = worker_poolFaulty(Mappers),
    io:format(" Mappeds"),
    Reducers = [reducerPool(Reduce, I, Mappeds) || I <- lists:seq(0, R-1)],
    io:format(" Reducers"),
    Reduceds = worker_poolFaulty(Reducers),
    io:format(" reduceds~n"),
    lists:sort(lists:flatten(Reduceds)).
