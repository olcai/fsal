%% -*- mode: erlang;erlang-indent-level: 4;indent-tabs-mode: nil -*-
%% ex: ts=4 sw=4 et
%% ===================================================================
%% @copyright 2012 Erik Timan
%% @author Erik Timan <dev@timan.info>
%%
%% @doc
%% This module implements a (very) simple Amazon S3 backend for FSAL.
%% @end
%% ===================================================================
-module(amazon_fsal_backend).

%% ===================================================================
%% API function exports
%% ===================================================================

-export([init/1,
         put/4,
         put_direct/4,
         get/3,
         get_ignore_body/3,
         move/5,
         delete/3,
         list/2]).

%% ===================================================================
%% API function definitions
%% ===================================================================

-record(state, {token, bucket}).

init(Args) ->
    %% All arguments are mandatory
    Host = proplists:get_value(host, Args),
    Port = proplists:get_value(port, Args),
    SSL = proplists:get_value(ssl, Args),
    KeyID = proplists:get_value(uid, Args),
    Secret = proplists:get_value(secret, Args),
    BStyle = proplists:get_value(bucket_style, Args),
    Token = amazon_api:get_token(Host, Port, SSL, KeyID, Secret, BStyle),
    Bucket = proplists:get_value(bucket, Args),
    {ok, #state{token=Token, bucket=Bucket}}.

put(Path, FileName, Content, #state{token=Token, bucket=Bucket}=State) ->
    Name = filename:join([Path, FileName]),
    Reply =
        case amazon_api:put_object(Token, Bucket, Name, Content, []) of
            {ok, _Etag} ->
                ok
        end,
    {Reply, State}.

put_direct(Path,
           FileName,
           SourceFile,
           #state{token=Token, bucket=Bucket}=State) ->
    Name = filename:join([Path, FileName]),
    Reply =
        case amazon_api:put_object_sendfile(Token,
                                            Bucket,
                                            Name,
                                            SourceFile,
                                            []) of
            {ok, _Etag} ->
                ok
        end,
    {Reply, State}.

get(Path, FileName, #state{token=Token, bucket=Bucket}=State) ->
    Name = filename:join([Path, FileName]),
    %% We use an internal function that also can list, see below
    %{file, Data, _Metadata, _Headers} =
    %    amazon_api:get_object(Token, Bucket, Name, false, omit) of
    {get_ns(Name, Token, Bucket), State}.

%% This function fetches a file without returning the body. Useful for
%% benchmarking purposes.
get_ignore_body(Path,
                FileName,
                #state{token=Token, bucket=Bucket}=State) ->
    Name = filename:join([Path, FileName]),
    %% Originally we used the out-commented code here so that Hackney
    %% was used for getting without the body. Hackney was slow with
    %% SSL (probably due to me not using its socket pool), so we
    %% instead use the support for chunked downloads by lhttpc.
    %{file, Size, _Metadata, _Headers} = %
    %% amazon_api:get_object_ignore_body(Token, Bucket, Name, omit),
    {file, Pid, _Metadata, _Headers} =
        amazon_api:get_object(Token, Bucket, Name, true, omit),
    %% Loop over get_next and sum up the bytes returned.
    LoopFun =
        fun(Func, Acc) ->
                case amazon_api:get_next(Pid) of
                    {ok, final} -> Acc;
                    {ok, BodyPart} -> Func(Func, Acc+size(BodyPart))
                end
        end,
    Size = LoopFun(LoopFun, 0),
    {{file, Size}, State}.

%% TODO: Implement some kind of move operation (upload+delete?)
move(_OldPath, _OldFileName, _NewPath, _NewFileName,
     #state{token=_Token, bucket=_Bucket}=State) ->
    {not_implemented, State}.

delete(Path, FileName, #state{token=Token, bucket=Bucket}=State) ->
    Name = filename:join([Path, FileName]),
    Reply = case amazon_api:delete_object(Token, Bucket, Name) of
                ok ->
                    ok
            end,
    {Reply, State}.

list(Prefix, #state{token=Token, bucket=Bucket}=State) ->
    Reply = case amazon_api:list_objects(Token, Bucket, Prefix) of
                {ok, Objects} ->
                    {ok, Objects}
    end,
    {Reply, State}.

%% ===================================================================
%% Internal function definitions
%% ===================================================================

-spec get_ns(string(), any(), string()) ->
    {file | dirlist, Data :: binary() | pid() | [{string(), string() | list()}]}.
get_ns(Name, Token, Bucket) ->
    case amazon_api:get_object(Token, Bucket, Name, false, omit) of
        {file, Data, _Metadata, _Headers} ->
            {file, Data};
        {error, amazon, "NoSuchKey", _Msg} ->
            %% This means that we should do a listing instead
            case amazon_api:list_objects(Token, Bucket, Name) of
                {ok, Objects} ->
                    {dirlist, Objects};
                Error ->
                    Error
            end
    end.
