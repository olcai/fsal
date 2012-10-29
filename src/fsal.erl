%% -*- mode: erlang;erlang-indent-level: 4;indent-tabs-mode: nil -*-
%% ex: ts=4 sw=4 et
%% ===================================================================
%% @copyright 2012 Erik Timan
%% @author Erik Timan <dev@timan.info>
%%
%% @doc
%% This module implements a simple File System Abstraction Layer.
%% @end
%% ===================================================================
-module(fsal).

-record(state, {bstate, dispatcher}).

%% ===================================================================
%% API function exports
%% ===================================================================

-export([start/0,
         stop/0,
         init/1,
         put/4,
         put_direct/4,
         get/3,
         move/5,
         delete/3]).

%% ===================================================================
%% API function definitions
%% ===================================================================

%% @doc Helper for starting the application from the shell.
start() ->
    %% SSL needs special start
    ssl:start(),
    %% Start our other dependencies as needed
    Deps = [crypto, lhttpc, hackney],
    [ start_ok(App, application:start(App)) || App <- Deps ],
    %% Ok, start ourselves!
    application:start(?MODULE).

%% @doc Helper for stopping the application from the shell.
stop()  ->
    application:stop(?MODULE).

init(Args) ->
    %% Setup the backend (dispatcher) module and init it.
    Dispatcher = case proplists:get_value(backend, Args) of
                     amazon -> amazon_fsal_backend;
                     file -> file_fsal_backend;
                     nilfs -> nilfs_fsal_backend
                 end,
    {ok, BState} = Dispatcher:init(
                     proplists:get_value(backend_args, Args)),

    {ok, #state{bstate=BState, dispatcher=Dispatcher}}.

put(Path, FileName, Content,
    #state{bstate=BState, dispatcher=Dispatcher}=State) ->
    {Ret, NBState} = Dispatcher:put(Path, FileName, Content, BState),
    {ok, Ret, State#state{bstate=NBState}}.

put_direct(Path, FileName, SourceFile,
           #state{bstate=BState, dispatcher=Dispatcher}=State) ->
    {Ret, NBState} =
        Dispatcher:put_direct(Path, FileName, SourceFile, BState),
    {ok, Ret, State#state{bstate=NBState}}.

get(Path, FileName,
    #state{bstate=BState, dispatcher=Dispatcher}=State) ->
    {Ret, NBState} = Dispatcher:get(Path, FileName, BState),
    {ok, Ret, State#state{bstate=NBState}}.

move(OldPath, OldFileName, NewPath, NewFileName,
     #state{bstate=BState, dispatcher=Dispatcher}=State) ->
    {Ret, NBState} =
        Dispatcher:move(
          OldPath, OldFileName, NewPath, NewFileName, BState),
    {ok, Ret, State#state{bstate=NBState}}.

delete(Path, FileName,
       #state{bstate=BState, dispatcher=Dispatcher}=State) ->
    {Ret, NBState} = Dispatcher:delete(Path, FileName, BState),
    {ok, Ret, State#state{bstate=NBState}}.

%% ===================================================================
%% Internal function definitions
%% ===================================================================

start_ok(_App, ok) -> ok;
start_ok(_App, {error, {already_started, _App}}) -> ok;
start_ok(App, {error, Reason}) ->
    erlang:error({app_start_failed, App, Reason}).
