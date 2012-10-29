%% -*- mode: erlang;erlang-indent-level: 4;indent-tabs-mode: nil -*-
%% ex: ts=4 sw=4 et
%% ===================================================================
%% @copyright 2012 Erik Timan
%% @author Erik Timan <dev@timan.info>
%%
%% @doc
%% This module implements a dummy backend for FSAL.
%% @end
%% ===================================================================
-module(nilfs_fsal_backend).

%% ===================================================================
%% API function exports
%% ===================================================================

-export([init/1, put/4, put_direct/4, get/3, move/5, delete/3]).

%% ===================================================================
%% API function definitions
%% ===================================================================

init(_Args) ->
    {ok, []}.

put(Path, FileName, _Content, State) ->
    _Name = filename:join([Path, FileName]),
    {ok, State}.

put_direct(Path, FileName, _SourceFile, State) ->
    _Name = filename:join([Path, FileName]),
    {ok, State}.

get(Path, FileName, State) ->
    _Name = filename:join([Path, FileName]),
    {ok, State}.

move(_OldPath, _OldFileName, _NewPath, _NewFileName, State) ->
    {ok, State}.

delete(Path, FileName, State) ->
    _Name = filename:join([Path, FileName]),
    {ok, State}.
