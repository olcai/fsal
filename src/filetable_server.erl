%% -*- mode: erlang;erlang-indent-level: 4;indent-tabs-mode: nil -*-
%% ex: ts=4 sw=4 et
%% ===================================================================
%% @copyright 2012 Erik Timan
%% @author Erik Timan <dev@timan.info>
%%
%% @doc
%% This module implements a simple metadata store for file data. It
%% keeps track of written files and their sizes.
%% @end
%% ===================================================================
-module(filetable_server).
-behaviour(gen_server).

%% Used for read_file_info
-include_lib("kernel/include/file.hrl").

-define(TABLE_NAME, filetable_table).

%% ===================================================================
%% API function exports
%% ===================================================================

-export([start/1, start_link/1, stop/0]).

-export([walk/1,
         insert_file/4,
         get_all/0,
         get_next_write/0,
         get_next_read/0,
         get_random/0,
         save/0]).

%% ===================================================================
%% gen_server function exports
%% ===================================================================

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% ===================================================================
%% API function definitions
%% ===================================================================

start(Args) ->
    gen_server:start({local, ?MODULE}, ?MODULE, Args, []).

start_link(Args) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).

stop() -> gen_server:cast(?MODULE, stop).

%% Walks a tree and inserts all files in that tree (and all subtrees)
%% into our ETS table.
walk(Path) ->
    gen_server:call(?MODULE, {walk, Path}, infinity).

%% Set WriteFlag to true if the file has been written to disk.
insert_file(RelPath, FileName, Size, WriteFlag) ->
    gen_server:call(?MODULE,
                    {insert_file, RelPath, FileName, Size, WriteFlag}).

%% The data is stored as {{Type, BasePath, RelPath, Filename}, {Size,
%% WriteFlag} in our tuple store.

%% Returns all entries in the ETS table.
get_all() ->
    gen_server:call(?MODULE, get_all).

%% Will return tuple or the atom none.
get_next_write() ->
    gen_server:call(?MODULE, {get_next, write}).

%% Will return tuple or the atom none.
get_next_read() ->
    gen_server:call(?MODULE, {get_next, read}).

%% Returns a random entry in the table.
get_random() ->
    gen_server:call(?MODULE, get_random).

%% Saves the table to disk.
save() ->
    gen_server:call(?MODULE, save).

%% ===================================================================
%% gen_server callbacks
%% ===================================================================

-record(state, {table, filename, write_cont, read_cont, walked}).

init(Args) ->
    erlang:process_flag(trap_exit, true),
    %% Filename is the name of where the ETS table will be stored when
    %% saving it
    Filename = proplists:get_value(filename, Args),
    %% The InputFilename is the name of the ETS file that will be
    %% loaded on startup. The reason for having two different
    %% filenames is that we generally doesn't want to load and save to
    %% the same file.
    InputFilename = proplists:get_value(input_filename, Args),
    %% We set Walked to true if we're able to load the table since a
    %% new walk will overwrite the old info.
    {Table, Walked} =
        case ets:file2tab(InputFilename) of
            {ok, Tab} ->
                {Tab, true};
            {error, _Reason} ->
                {ets:new(?TABLE_NAME, [set, compressed]), false}
        end,
    {ok, #state{table=Table,
                filename=Filename,
                write_cont=none,
                read_cont=none,
                walked=Walked}}.

handle_call({walk, Path},
            _From,
            #state{table=Table, walked=Walked}=State) ->
    %% Check if we've already walked the tree
    case Walked of
        true ->
            {reply, ok, State};
        false ->
            {reply, walk(Table, Path, Path), State#state{walked=true}}
    end;
handle_call({insert_file, RelPath, FileName, Size, WriteFlag},
            _From,
            #state{table=Table}=State) ->
    true = ets:insert(Table, {{file, <<>>, RelPath, FileName},
                              {Size, WriteFlag}}),
    {reply, ok, State};
handle_call(get_all, _From, #state{table=Table}=State) ->
    %% Returns [{Type, Path, Name, Size, WriteFlag}]
    Reply = ets:select(Table, get_matchspec()),
    {reply, Reply, State};
handle_call({get_next, Type}, _From, #state{table=Table}=State) ->
    %% Returns {Type, BasePath, RelPath, Name, Size} or none
    %% Select the type of continuation to work with
    {Cont, MatchSpec} =
        case Type of
            read ->
                %% Here we match on the WriteFlag (i.e. only return
                %% already written elements)
                {State#state.read_cont, get_matchspec(true)};
            write ->
                {State#state.write_cont, get_matchspec()}
        end,
    SR = case Cont of
             none -> ets:select(Table, MatchSpec, 1);
             completed -> completed;
             C -> ets:select(C)
         end,
    {Reply, NC} = case SR of
                      {[Match], Continuation} ->
                          %% Update the WriteFlag in table
                          {T, BP, RP, FN, S, _WF} = Match,
                          ets:insert(Table, {{T, BP, RP, FN}, {S,true}}),
                          %% Return
                          {Match, Continuation};
                      '$end_of_table' ->
                          {none, completed};
                      completed ->
                          {none, completed}
                  end,
    NewState = case Type of
                   read -> State#state{read_cont=NC};
                   write -> State#state{write_cont=NC}
               end,
    {reply, Reply, NewState};
handle_call(get_random, _From, #state{table=Table}=State) ->
    %% Returns {Type, BasePath, RelPath, Name, Size} or none
    Reply =
        case ets:info(Table, size) of
            0 -> none;
            Size ->
                random:seed(),
                case random:uniform(Size)-1 of
                    N when N < 0 ->
                        none;
                    N ->
                        {{Type, BasePath, RelPath, FileName},
                         {Size, WriteF}} =
                            ets:slot(Table, N),
                        {Type, BasePath, RelPath, FileName, Size, WriteF}
                end
        end,
    {reply, Reply, State};
handle_call(delete_all, _From, #state{table=Table}=State) ->
    true = ets:match_delete(Table, {{'$1','$2','$3','$4'},{'$5','$6'}}),
    {reply, ok, State};
handle_call({save, Filename}, _From, #state{table=Table}=State) ->
    ok = ets:tab2file(Table, Filename),
    {reply, ok, State};
handle_call(save, _From, #state{table=Table,filename=Filename}=State) ->
    ok = ets:tab2file(Table, Filename),
    {reply, ok, State};
handle_call({load, Filename}, _From, #state{table=Table}=State) ->
    %% WARNING: Will replace current ETS table with the loaded one
    Reply = case ets:file2tab(Filename) of
                {ok, NewTable} ->
                    true = ets:delete(Table),
                    {reply, ok, #state{table=NewTable}};
                {error, _Reason} ->
                    {reply, error, State}
            end,
    Reply;
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(stop, State) ->
    {stop, shutdown, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ===================================================================
%% Internal function definitions
%% ===================================================================

%% Return file type (device, directory, regular, other)
file_type(Path) ->
    %% Get the file type
    {ok, #file_info{type=Type}} = file:read_file_info(Path),
    Type.

%% Returns a splitted version of Path into {Prefix, RelativePath}. The
%% point of this function is simply to split properly while ensuring
%% that no errors occur.
split_on_basepath(Path, BasePath) ->
    %% Ensure that the basepath doesn't have an ending slash
    BP = filename:nativename(BasePath),
    case lists:prefix(BP, Path) of
        true ->
            %% We split on length+1 here so that the directory
            %% separator (slash) ends up in the prefix part
            {Prefix, RP} = lists:split(length(BP)+1, Path),
            {Prefix, RP};
        false ->
            erlang:error(basepath_is_not_a_prefix_of_path)
    end.
    
%% Walk a file tree rooted at Path and insert each file into
%% Table. The BasePath should be the same as Path for the initial
%% call. It is used to split the path entered in the ETS table into a
%% relative component as well.
walk(Table, Path, BasePath) ->
    case file_type(Path) of
        regular ->
            {Prefix, RelativePath} =
                split_on_basepath(filename:dirname(Path), BasePath),
            Obj = {{file, Prefix, RelativePath, filename:basename(Path)},
                   {filelib:file_size(Path), false}},
            true = ets:insert(Table, Obj);
        directory ->
            Children = filelib:wildcard(Path ++ "/*"),
            lists:foreach(fun(P) -> walk(Table, P, BasePath) end,
                          Children);
        _Else ->
            ok
    end.

%% The general MatchSpec.
get_matchspec() ->
    [{{{'$1','$2','$3','$4'},{'$5','$6'}},
      [],
      [{{'$1','$2','$3','$4','$5','$6'}}]}].
get_matchspec(WriteFlag) ->
    [{{{'$1','$2','$3','$4'},{'$5',WriteFlag}},
      [],
      [{{'$1','$2','$3','$4','$5',WriteFlag}}]}].
