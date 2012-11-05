%% -*- mode: erlang;erlang-indent-level: 4;indent-tabs-mode: nil -*-
%% ex: ts=4 sw=4 et
%% ===================================================================
%% @copyright 2012 Erik Timan
%% @author Erik Timan <dev@timan.info>
%%
%% @doc
%% This module implements a simple file backend for FSAL.
%%
%% A special feature is that it supports partitioning writes over
%% several directories by looking at the first character in the file
%% name of a file.
%%
%% @end
%% ===================================================================
-module(file_fsal_backend).

%% We define the alphabet used for partitioning writes to different
%% BasePaths. Note the file names should ONLY contain these chars.
-define(ALPHABET, "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789").

%% ===================================================================
%% API function exports
%% ===================================================================

-export([init/1,
         put/4,
         put_direct/4,
         get/3,
         get_ignore_body/3,
         move/5,
         delete/3]).

%% ===================================================================
%% API function definitions
%% ===================================================================

-record(state, {basepaths, map}).

init(Args) ->
    %% BasePaths is a list of paths to which we will write files (a
    %% prefix before file paths and names). It is expected to be a
    %% list of strings. If more than one basepath is given, writes
    %% will be distributed between the paths using a partitioning
    %% scheme based on the file name of a file. The rationale for this
    %% is to devide load between different mount points and so that we
    %% can deterministacally always know where a file has been put
    %% without saving this information.
    BasePaths = proplists:get_value(basepaths, Args),
    Map = gen_mapping(?ALPHABET, BasePaths),
    {ok, #state{basepaths=BasePaths, map=Map}}.

%% Returns ok | {error, Reason}
put(Path, FileName, Content, State) ->
    {BP, NState} = get_basepath(State, FileName),
    Name = filename_join([BP, Path, FileName]),
    %% Note that we use raw access here
    Ret = case file:write_file(Name, Content, [raw]) of
              ok -> ok;
              {error, enoent} ->
                  ok = filelib:ensure_dir(Name),
                  file:write_file(Name, Content, [raw]);
              Else -> Else
          end,
    {Ret, NState}.

put_direct(Path, FileName, SourceFile, State) ->
    {BP, NState} = get_basepath(State, FileName),
    %% SourceFile is expected to have a full path in it
    DestName = filename_join([BP, Path, FileName]),
    %% Note that we use raw access here
    Ret = case file:copy({SourceFile, [read, binary, raw]},
                         {DestName, [write, binary, raw]}) of
              {ok, _BytesCopied} -> ok;
              {error, enoent} ->
                  ok = filelib:ensure_dir(DestName),
                  R = file:copy({SourceFile, [read, binary, raw]},
                                {DestName, [write, binary, raw]}),
                  case R of
                      {ok, _BytesCopied} -> ok;
                      Err -> Err
                  end;
              Else -> Else
          end,
    {Ret, NState}.

%% Returns {{file, Binary}, State} | {{error, Reason}, State}
get(Path, FileName, State) ->
    {BP, NState} = get_basepath(State, FileName),
    Name = filename_join([BP, Path, FileName]),
    %% Note that we use the file server here...
    Ret = case file:read_file(Name) of
              {ok, Data} -> {file, Data};
              Err -> Err
          end,
    {Ret, NState}.

%% Returns {{file, Size}, State} | {{error, Reason}, State}
get_ignore_body(Path, FileName, State) ->
    {BP, NState} = get_basepath(State, FileName),
    Name = filename_join([BP, Path, FileName]),
    %% We use a fun here that loops and reads from the given IoDevice
    %% in chunks.
    LoopFun =
        fun(Fun, IoDevice, Acc) ->
                case file:read(IoDevice, 1000000) of
                    {ok, Data} ->
                        Fun(Fun, IoDevice, Acc+byte_size(Data));
                    eof ->
                        {file, Acc};
                    Else ->
                        Else
                end
        end,
    %% Note that we use raw file access here
    Ret = case file:open(Name, [read, raw, binary]) of
              {ok, IoDevice} ->
                  R = LoopFun(LoopFun, IoDevice, 0),
                  file:close(IoDevice),
                  R;
              Err -> Err
          end,
    {Ret, NState}.

%% Returns {ok, State} | {{error, Reason}, State}
move(OldPath, OldFileName, NewPath, NewFileName, State) ->
    {OldBP, NState1} = get_basepath(State, OldFileName),
    {NewBP, NState2} = get_basepath(NState1, NewFileName),
    Source = filename_join([OldBP, OldPath, OldFileName]),
    Destination = filename_join([NewBP, NewPath, NewFileName]),
    Ret = file:rename(Source, Destination),
    {Ret, NState2}.

%% Returns {ok, State} | {{error, Reason}, State}
delete(Path, FileName, State) ->
    {BP, NState} = get_basepath(State, FileName),
    Name = filename_join([BP, Path, FileName]),
    Ret = file:delete(Name),
    {Ret, NState}.

%% ===================================================================
%% Internal function definitions
%% ===================================================================

%% These join functions should work like filename:join, except that
%% any intermediate dirs of type absolute won't overwrite the
%% preceeding elements. I.e. filename_join("aa", "/bb") will return
%% "aa/bb" NOT "/bb".
filename_join([Name1, Name2|Rest]) ->
    filename_join([filename_join(Name1, Name2)|Rest]);
filename_join([Name]) ->
    Name.

filename_join(Name1, Name2) ->
    case filename:pathtype(Name2) of
        absolute ->
            [_ | RelDir] = filename:split(Name2),
            filename:join([Name1] ++ RelDir);
        _ ->
            filename:join([Name1, Name2])
    end.

%% The idea is to select a basepath for the given filename using a
%% simple partitioning technique.
get_basepath(#state{map=Map}=State, FileName) ->
    %% Get the initial char in FileName
    FL = binary_to_list(iolist_to_binary(FileName)),
    [IC|_T] = FL,
    {ok, BasePath} = orddict:find(IC, Map),
    {BasePath, State}.

%% Get alphabet from string, and returns an orddict mapping chars from
%% the alphabet as keys to a basepath as a value
gen_mapping(Alphabet, Basepaths) ->
    NbrP = length(Basepaths),
    Chunks = split_in_n_chunks(Alphabet, NbrP),
    L = lists:append(
          [
           [ {Char, Basepath} || Char <- Chunk ]
           || {Chunk, Basepath} <- lists:zip(Chunks, Basepaths)
          ]),
    orddict:from_list(L).

%% Split a list in N chunks. The last sub-list may be longer then the
%% previous sub-lists. If N is larger than the number of elements in
%% L, a best-effort split will be made, but a smaller number of chunks
%% the requested will be provided.
split_in_n_chunks(L, N) ->
    ChunkSize = if N >= length(L) -> 1;
                   true -> length(L) div N
                end,
    %ChunkSize = length(L) div N,
    partition(L, ChunkSize, N, []).

%% Partition a list, L, in partitions of size ChunkSize, with the
%% number of chunks left (the expected number of chunks).
partition([], _ChunkSize, _NbrChunksLeft, Acc) ->
    lists:reverse(Acc);
partition(L, _ChunkSize, 1, Acc) ->
    lists:reverse([L|Acc]);
partition(L, ChunkSize, NbrChunksLeft, Acc) when NbrChunksLeft > 1 ->
    {Part, Rest} = lists:split(ChunkSize, L),
    partition(Rest, ChunkSize, NbrChunksLeft-1, [Part|Acc]).
