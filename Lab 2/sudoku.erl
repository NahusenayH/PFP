-module(sudoku).

%-include_lib("eqc/include/eqc.hrl").
-compile(export_all).

%% %% generators

%% matrix(M,N) ->
%%     vector(M,vector(N,nat())).

%% matrix transpose

transpose([Row]) ->
    [[X] || X <- Row];
transpose([Row | M]) ->
    [[X | Xs] || {X, Xs} <- lists:zip(Row, transpose(M))].

%% prop_transpose() ->
%%     ?FORALL({M,N},{nat(),nat()},
%%             ?FORALL(Mat,matrix(M+1,N+1),
%%                     transpose(transpose(Mat)) == Mat)).

%% map a matrix to a list of 3x3 blocks, each represented by the list
%% of elements in row order

triples([A, B, C | D]) ->
    [[A, B, C] | triples(D)];
triples([]) ->
    [].

blocks(M) ->
    Blocks = [triples(X) || X <- transpose([triples(Row) || Row <- M])],
    lists:append(
        lists:map(fun(X) -> lists:map(fun lists:append/1, X) end, Blocks)).

unblocks(M) ->
    lists:map(fun lists:append/1,
              transpose(lists:map(fun lists:append/1,
                                  lists:map(fun(X) -> lists:map(fun triples/1, X) end,
                                            triples(M))))).

%% prop_blocks() ->
%%     ?FORALL(M,matrix(9,9),
%%             unblocks(blocks(M)) == M).

%% decide whether a position is safe

entries(Row) ->
    [X || X <- Row, 1 =< X andalso X =< 9].

safe_entries(Row) ->
    Entries = entries(Row),
    lists:sort(Entries) == lists:usort(Entries).

safe_rows(M) ->
    lists:all(fun safe_entries/1, M).

safe(M) ->
    safe_rows(M) andalso safe_rows(transpose(M)) andalso safe_rows(blocks(M)).

%% fill blank entries with a list of all possible values 1..9

fill(M) ->
    Nine = lists:seq(1, 9),
    [[if 1 =< X, X =< 9 ->
             X;
         true ->
             Nine
      end
      || X <- Row]
     || Row <- M].

%% refine entries which are lists by removing numbers they are known
%% not to be

refine(M) ->
    NewM = refine_rows(transpose(refine_rows(transpose(unblocks(refine_rows(blocks(M))))))),
    if M == NewM ->
           M;
       true ->
           refine(NewM)
    end.

refine_rows(M) ->
    lists:map(fun refine_row/1, M).

refine_row(Row) ->
    Entries = entries(Row),
    NewRow =
        [if is_list(X) ->
                case X -- Entries of
                    [] ->
                        exit(no_solution);
                    [Y] ->
                        Y;
                    NewX ->
                        NewX
                end;
            true ->
                X
         end
         || X <- Row],
    NewEntries = entries(NewRow),
    %% check we didn't create a duplicate entry
    case length(lists:usort(NewEntries)) == length(NewEntries) of
        true ->
            NewRow;
        false ->
            exit(no_solution)
    end.

is_exit({'EXIT', _}) ->
    true;
is_exit(_) ->
    false.

%% is a puzzle solved?

solved(M) ->
    lists:all(fun solved_row/1, M).

solved_row(Row) ->
    lists:all(fun(X) -> 1 =< X andalso X =< 9 end, Row).

%% how hard is the puzzle?

hard(M) ->
    lists:sum([lists:sum([if is_list(X) ->
                                 length(X);
                             true ->
                                 0
                          end
                          || X <- Row])
               || Row <- M]).

%% choose a position {I,J,Guesses} to guess an element, with the
%% fewest possible choices

guess(M) ->
    Nine = lists:seq(1, 9),
    {_, I, J, X} =
        lists:min([{length(X), I, J, X}
                   || {I, Row} <- lists:zip(Nine, M), {J, X} <- lists:zip(Nine, Row), is_list(X)]),
    {I, J, X}.

%% given a matrix, guess an element to form a list of possible
%% extended matrices, easiest problem first.

guesses(M) ->
    {I, J, Guesses} = guess(M),
    Ms = [catch refine(update_element(M, I, J, G)) || G <- Guesses],
    SortedGuesses = lists:sort([{hard(NewM), NewM} || NewM <- Ms, not is_exit(NewM)]),
    [G || {_, G} <- SortedGuesses].

update_element(M, I, J, G) ->
    update_nth(I, update_nth(J, G, lists:nth(I, M)), M).

update_nth(I, X, Xs) ->
    {Pre, [_ | Post]} = lists:split(I - 1, Xs),
    Pre ++ [X | Post].

%% prop_update() ->
%%     ?FORALL(L,list(int()),
%%             ?IMPLIES(L/=[],
%%                      ?FORALL(I,choose(1,length(L)),
%%                              update_nth(I,lists:nth(I,L),L) == L))).

%% solve a puzzle

solve(M) ->
    Solution = solve_refined(refine(fill(M))),
    case valid_solution(Solution) of
        true ->
            Solution;
        false ->
            exit({invalid_solution, Solution})
    end.

solve_refined(M) ->
    case solved(M) of
        true ->
            M;
        false ->
            solve_one(guesses(M))
    end.

solve_one([]) ->
    exit(no_solution);
solve_one([M]) ->
    solve_refined(M);
solve_one([M | Ms]) ->
    case catch solve_refined(M) of
        {'EXIT', no_solution} ->
            solve_one(Ms);
        Solution ->
            Solution
    end.

%% benchmarks

-define(EXECUTIONS, 100). %%%%%%%%%%%%

bm(F) ->
    {T, _} = timer:tc(?MODULE, repeat, [F]),
    T / ?EXECUTIONS / 1000.

repeat(F) ->
    [F() || _ <- lists:seq(1, ?EXECUTIONS)].

benchmarks(Puzzles) ->
    [{Name, bm(fun() -> solve(M) end)} || {Name, M} <- Puzzles].

benchmarks() ->
    {ok, Puzzles} = file:consult("problems.txt"),
    timer:tc(?MODULE, benchmarks, [Puzzles]).

%% check solutions for validity

valid_rows(M) ->
    lists:all(fun valid_row/1, M).

valid_row(Row) ->
    lists:usort(Row) == lists:seq(1, 9).

valid_solution(M) ->
    valid_rows(M) andalso valid_rows(transpose(M)) andalso valid_rows(blocks(M)).

%%--------------------------------------------------------------

%%---- Running the parallelized benchmarking function
%%    (Remember to put the right benchmarkingPar function inside like benchmarksPar2 et cetera.)
benchmarksPar() ->
    {ok, Puzzles} = file:consult("problems.txt"),
    timer:tc(?MODULE, benchmarksPar, [Puzzles]).

benchmarksPar(Puzzles) ->
    [{Name, bm(fun() -> solvePar(M) end)} || {Name, M} <- Puzzles].

solvePar(M) ->
    Solution = solve_refined(refinePar(fill(M))),
    case valid_solution(Solution) of
        true ->
            Solution;
        false ->
            exit({invalid_solution, Solution})
    end.

refinePar(M) ->
    NewM =
        refine_rowsPar(transpose(refine_rowsPar(transpose(unblocks(refine_rowsPar(blocks(M))))))),
    if M == NewM ->
           M;
       true ->
           refinePar(NewM)
    end.

%%---- Refining rows in parallel
refine_rowsPar(M) ->
    mapParallell(M, fun refine_row/1).

%%---- Map function parallelized
mapParallell(List, Function) ->
    Root = self(),
    [receive
         {Pid, Result} ->
             Result
     end
     || Pid <- [spawn(fun() -> Root ! {self(), Function(Element)} end) || Element <- List]].


%%---- Benchmarking in parallel (remember to put this function in benchmarkPar() to use.

benchmarksPar2(Puzzles) ->
    Root = self(),
    [{receive
          {Pid, Result} ->
              Result
      end}
     || Pid
            <- [spawn(fun() -> Root ! {self(), {Name, bm(fun() -> solve(M) end)}} end)
                || {Name, M} <- Puzzles]].




%%---- Guessing in parallel

%% Most functions here are the same but we copied them here for easier running in the terminal without changing the function used for benchmarks
benchmarksPar4() ->
    {ok, Puzzles} = file:consult("problems.txt"),
    timer:tc(?MODULE, benchmarksPar4, [Puzzles]).

benchmarksPar4(Puzzles) ->
    [{Name, bm(fun() -> solvePar4(M) end)} || {Name, M} <- Puzzles].

solvePar4(M) ->
    Solution = solve_refinedPar4(refine(fill(M))),
    case valid_solution(Solution) of
        true ->
            Solution;
        false ->
            exit({invalid_solution, Solution})
    end.

solve_refinedPar4(M) ->
    case solved(M) of
        true ->
            M;
        false ->
            solve_onePar4(guesses(M))
    end.

%% We changed this function to solve all the guesses in parallel we spawn an arbiter
%% that recieves the messages from all the processes solving the guesses and when it receives a solution
%% sends it back to the origin process.
solve_onePar4([]) ->
    exit(no_solution);
solve_onePar4([M]) ->
    solve_refined(M);
solve_onePar4([M | Ms]) ->
    Root = self(),
    ArbiterPid = spawn(fun() -> arbiter4(Root) end),
    mapParallell4([M | Ms], fun solve_one/1, ArbiterPid),
    receive
        {Pid, Result} ->
            case Result of
                no_solution ->
                    Pid ! {Root, ok};
                Solution ->
                    Solution
            end
    end.

%% Inspired by our parallel map function it spawns a new process for each guess using list comprehension
%% in the list of guesses and attempts to solve it, sending the result (solution or exit)
%% to the arbiter.
mapParallell4(List, Function, Root) ->
    [spawn(fun() -> Root ! {self(), catch Function([Element])} end) || Element <- List].

%% Receives the messages from the processes, judges them and if it is a solution sends it to the origin/parent process.
arbiter4(Root) ->
    receive
        {Pid, Result} ->
            case Result of
                {'EXIT', _} ->
                    arbiter4(Root);
                Solution ->
                    Root ! {self(), Solution}
            end
    end.
