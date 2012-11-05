%% -*- mode: erlang;erlang-indent-level: 4;indent-tabs-mode: nil -*-
%% ex: ts=4 sw=4 et
%% ===================================================================
%% @copyright 2012 Erik Timan
%% @author Erik Timan <dev@timan.info>
%%
%% @doc
%% This is a simple wrapper for the Amazon S3 REST API. It implements
%% only basic operations on Amazon S3.
%%
%% Operations currently not supported: some bucket operations,
%% Multiple Object Delete, ACLs, versioning, multipart upload and
%% object copying.
%%
%% The interface is used by first creating a token with get_token/5
%% and then using that token in each request.
%% @end
%% ===================================================================
-module(amazon_api).

%% README


%% Timeout for requests in ms
-define(TIMEOUT, 300000).

-record(token, {host :: string(),
                port :: integer(),
                ssl :: boolean(),
                keyid :: string(),
                secret :: string(),
                bucket_style :: path | vhost
                }).
-opaque token() :: #token{}.

-record(request, {host :: string(),
                  port :: integer(),
                  ssl :: boolean(),
                  resource :: string(),
                  method :: string(),
                  headers :: list(),
                  body :: iolist(),
                  timeout :: integer(),
                  options :: list()}).

-type header() :: {string(), string()}.

-type error() :: {error, amazon, ErrorCode :: integer(), ErrorMsg :: string()} |
                 {error, amazon, ResponseBody :: binary()} |
                 {error, http, StatusCode :: integer(), ReasonPhrase :: string()} |
                 {error, connection_closed | connect_timeout | timeout}.

-type proplist() :: [{Key :: string(), Value :: string()}].

%% Include xmerl types
-include_lib("xmerl/include/xmerl.hrl").

%% ===================================================================
%% API function exports
%% ===================================================================

-export([get_token/6,
         put_object/5,
         put_object_sendfile/5,
         put_bucket/3,
         list_objects/3,
         list_buckets/1,
         delete_object/3,
         delete_bucket/2,
         get_object/5,
         get_object_metadata/3,
         get_object_ignore_body/4,
         get_next/1
        ]).

%% These are internal but exported for EUnit. Not for public use!
-export([get_keys_from_listing/1,
         convert_xml/1,
         extract_headers/2,
         get_metadata_from_headers/1,
         create_metadata_headers/1,
         sign/7,
         canonicalize_resource/2,
         canonicalize_headers/1,
         canonicalize_header/1]).

%% ===================================================================
%% API function definitions
%% ===================================================================

%% ------------------------------------------------------------------
%% @doc
%% Creates a record used in subsequent calls to the API with
%% connection information. KeyID is the Amazon Key ID and Secret is
%% the shared secret. BucketStyle indicates if we should use
%% path-style or virtual-host-style access to buckets.
%% 
%% @end
%% ------------------------------------------------------------------
-spec get_token(Host :: string(),
                Port :: integer(),
                SSL :: boolean(),
                KeyID :: string(),
                Secret :: string(),
                BucketStyle :: path | vhost) ->
    token().
get_token(Host, Port, SSL, KeyID, Secret, BucketStyle) ->
    #token{host=Host,
           port=Port,
           ssl=SSL,
           keyid=KeyID,
           secret=Secret,
           bucket_style=BucketStyle}.

%% ------------------------------------------------------------------
%% @doc
%% Creates an object, optionally with metadata. To not set any metadata, set the
%% metadata parameters to []. Note that every key in a metadata key/value-pair
%% will be converted to lower case.
%%
%% @end
%% ------------------------------------------------------------------
-spec put_object(token(),
                 Bucket :: string(),
                 ObjectName :: string(),
                 Data :: iolist(),
                 Metadata :: proplist()) ->
    {ok, InterestingHeaders :: proplist()} | error().
put_object(Token = #token{}, Bucket, ObjectName, Data, Metadata) ->
    AwsH = create_metadata_headers(Metadata),
    case put(Token, Bucket, "/"++ObjectName, AwsH, Data) of
        {ok, {{200, _ReasonPhrase}, Headers, <<>>}} ->
            {ok, extract_headers(Headers, ["Etag"])};
        Error ->
            handle_errors(Error)
    end.

%% ------------------------------------------------------------------
%% @doc
%% Creates an object, optionally with metadata, using the contents
%% from the given SourceFile. The file is sent using the sendfile API
%% (via the HTTP client Hackney).
%%
%% To not set any metadata, set the metadata parameters to []. Note
%% that every key in a metadata key/value-pair will be converted to
%% lower case.
%%
%% @end
%% ------------------------------------------------------------------
-spec put_object_sendfile(token(),
                          Bucket :: string(),
                          ObjectName :: string(),
                          SourceFile :: iolist(),
                          Metadata :: proplist()) ->
    {ok, InterestingHeaders :: proplist()} | error().
put_object_sendfile(Token = #token{},
                    Bucket,
                    ObjectName,
                    SourceFile,
                    Metadata) ->
    AwsH = create_metadata_headers(Metadata),
    Payload = {file, SourceFile},
    case put_hackney(Token, Bucket, "/"++ObjectName, AwsH, Payload) of
        {ok, {{200, _ReasonPhrase}, Headers, <<>>}} ->
            {ok, extract_headers(Headers, ["Etag"])};
        Error ->
            handle_errors(Error)
    end.

%% ------------------------------------------------------------------
%% @doc
%% Creates a bucket in the specified S3 region. The region is typically
%% something along the line of "EU", "us-west-1" etc. There are several
%% restrictions on how buckets can be named, please refer to the Amazon S3
%% documentation on "Bucket Restrictions and Limitations".
%%
%% @end
%% ------------------------------------------------------------------
-spec put_bucket(token(),
                 Bucket :: string(),
                 Region :: string()) ->
    ok | error().
put_bucket(Token = #token{}, Bucket, Region) ->
    Content =
        "<CreateBucketConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">
             <LocationConstraint>" ++ Region ++ "</LocationConstraint> 
           </CreateBucketConfiguration>",
    case put(Token, Bucket, "/", [], list_to_binary(Content)) of
        {ok, {{200, _ReasonPhrase}, _Headers, <<>>}} ->
            ok;
        Error ->
            handle_errors(Error)
    end.

%% ------------------------------------------------------------------
%% @doc
%% List all objects with the prefix Prefix. Returns a list of keys.
%%
%% @end
%% ------------------------------------------------------------------
-spec list_objects(token(), Bucket :: string(), Prefix :: string()) ->
    {ok, [Key :: string()]} | error().
list_objects(Token = #token{}, Bucket, Prefix) ->
    URL = case Prefix of
              "" -> "/";
              P -> "/" ++ "?prefix=" ++ P
          end,
    handle_listing(Token, Bucket, URL, "", []).

%% ------------------------------------------------------------------
%% @doc
%% List all buckets owned by the account. Returns a list of bucket names.
%%
%% @end
%% ------------------------------------------------------------------
-spec list_buckets(token()) -> {ok, [Name :: string()]} | error().
list_buckets(Token = #token{}) ->
    case get(Token, "", "/", [], "", false) of
        {ok, {{200, _ReasonPhrase}, _Headers, ResponseBody}} ->
            DocRootElement = get_docroot_from_xml(ResponseBody),
            %% Extract bucket names
            XMLBuckets = xmerl_xpath:string(
                           "/ListAllMyBucketsResult/Buckets/Bucket/Name/text()",
                           DocRootElement),
            %% Extract all bucket names
            {ok, [ Name || #xmlText{value=Name} <- XMLBuckets ]};
        Error ->
            handle_errors(Error)
    end.

%% ------------------------------------------------------------------
%% @doc
%% Deletes an object.
%%
%% @end
%% ------------------------------------------------------------------
-spec delete_object(token(), Bucket :: string(), ObjectName :: string()) ->
    ok | error().
delete_object(Token = #token{}, Bucket, ObjectName) ->
    case delete(Token, Bucket, "/"++ObjectName, []) of
        {ok, {{204, _ReasonPhrase}, _Headers, <<>>}} ->
            %% Note: Does not handle the returned headers from a versioned
            %% delete
            ok;
        Error ->
            handle_errors(Error)
    end.

%% ------------------------------------------------------------------
%% @doc
%% Deletes a bucket.
%%
%% @end
%% ------------------------------------------------------------------
-spec delete_bucket(token(), Bucket :: string()) ->
    ok | error().
delete_bucket(Token = #token{}, Bucket) ->
    case delete(Token, Bucket, "/", []) of
        {ok, {{204, _ReasonPhrase}, _Headers, <<>>}} ->
            ok;
        Error ->
            handle_errors(Error)
    end.

%% ------------------------------------------------------------------
%% @doc
%% Reads an object from S3. Use the Extent parameter for doing partial reads. It
%% works as the HTTP Range header, so "5-15" will return byte five to fifteen of
%% the object (inclusive). Things like "-10" also works (return the last ten
%% bytes of the object). Set the Extent parameter to 'omit' for reading the
%% whole object.
%%
%% By setting Chunked to true, chunked downloads will be used. The ResponseBody
%% will be a pid in that case. Use get_next/1 to get the next part of the
%% object.
%%
%% Note: There is no support for multiple Extent ranges such as "5-15,49-78". If
%% you use such ranges, the return value will be a non-parsed multipart
%% message. You have been warned.
%%
%% @end
%% ------------------------------------------------------------------
-spec get_object(token(),
                 Bucket :: string(),
                 ObjectName :: string(),
                 Chunked :: boolean(),
                 Extent :: string() | omit) ->
    {file, Data :: binary() | pid() | [{string(), string() | list()}],
     proplist(), proplist()} | error().
get_object(Token = #token{}, Bucket, ObjectName, Chunked, Extent) ->
    case Extent of
        omit -> Range = "";
        Val -> Range = "bytes="++Val
    end,
    URL = "/"++ObjectName,
    case get(Token, Bucket, URL, [], Range, Chunked) of
        {ok, {{HTTPCode, _ReasonPhrase}, Headers, ResponseBody}}
          when HTTPCode == 200; HTTPCode == 206 ->
            {file,
             ResponseBody,
             get_metadata_from_headers(Headers),
             extract_headers(Headers, ["Etag", "Last-Modified"])};
        Error ->
            handle_errors(Error)
    end.

%% ------------------------------------------------------------------
%% @doc
%% Queries S3 for all metadata associated with an object, including some
%% system-metadata from headers such as Etag.
%% 
%% @end
%% ------------------------------------------------------------------
-spec get_object_metadata(token(),
                          Bucket :: string(),
                          ObjectName :: string()) ->
    {ok, proplist(), proplist()} | error().
get_object_metadata(Token = #token{}, Bucket, ObjectName) ->
    URL = "/"++ObjectName,
    case head(Token, Bucket, URL, []) of
        {ok, {{200, _ReasonPhrase}, Headers, _ResponseBody}} ->
            {ok,
             get_metadata_from_headers(Headers),
             extract_headers(Headers, ["Etag", "Last-Modified"])};
        Error ->
            handle_errors(Error)
    end.

%% ------------------------------------------------------------------
%% @doc
%% Reads an object from S3, consumes the body but does not return
%% it. Useful for benchmarking purposes and uses the HTTP client
%% hackney instead of lhttpc. Use the Extent parameter for doing
%% partial reads. It works as the HTTP Range header, so "5-15" will
%% return byte five to fifteen of the object (inclusive). Things like
%% "-10" also works (return the last ten bytes of the object). Set the
%% Extent parameter to 'omit' for reading the whole object.
%%
%% Note: There is no support for multiple Extent ranges such as
%% "5-15,49-78". If you use such ranges, the return value will be a
%% non-parsed multipart message. You have been warned.
%%
%% @end
%% ------------------------------------------------------------------
-spec get_object_ignore_body(token(),
                             Bucket :: string(),
                             ObjectName :: string(),
                             Extent :: string() | omit) ->
    {file, Size :: integer() | [{string(), string() | list()}],
     proplist(), proplist()} | error().
get_object_ignore_body(Token = #token{}, Bucket, ObjectName, Extent) ->
    case Extent of
        omit -> Range = "";
        Val -> Range = "bytes="++Val
    end,
    URL = "/"++ObjectName,
    %% Define a function that reads the whole body from hackney, but
    %% in the end just returns the size of the read boddy,
    LoopFun = fun(Fun, Client1, AccSize) ->
                      case hackney:stream_body(Client1) of
                          {ok, Data, Client2} ->
                              Fun(Fun, Client2, AccSize+byte_size(Data));
                          {done, Client2} ->
                              hackney:close(Client2),
                              {ok, AccSize};
                          Else -> Else
                      end
              end,
    case get_hackney(Token, Bucket, URL, [], Range, true) of
        {client, {{HTTPCode, _ReasonPhrase}, Headers, Client}}
          when HTTPCode == 200; HTTPCode == 206 ->
            {ok, Size} = LoopFun(LoopFun, Client, 0),
            {file,
             Size,
             get_metadata_from_headers(Headers),
             extract_headers(Headers, ["Etag", "Last-Modified"])};
        Error ->
            handle_errors(Error)
    end.

%% ------------------------------------------------------------------
%% @doc
%% Gets the next part of a chunked download. The Pid is given by the
%% get_object function.
%%
%% @end
%% ------------------------------------------------------------------
-spec get_next(Pid :: pid()) -> {ok, final} | {ok, BodyPart :: binary()}.
get_next(Pid) ->
    case lhttpc:get_body_part(Pid, ?TIMEOUT) of
        {ok, {http_eob, _Trailers}} ->
            {ok, final};
        {ok, BodyPart} ->
            {ok, BodyPart}
    end.

%% ===================================================================
%% Internal function definitions
%% ===================================================================

%% ------------------------------------------------------------------
%% @doc
%% Extracts keys from ListBucketResult XML. This is not beautiful, but it
%% works. It will also return a boolean indicating if the listing is truncated
%% or not.
%%
%% @end
%% ------------------------------------------------------------------
-spec get_keys_from_listing(XML :: binary()) ->
    {ok, [Key :: string()], IsTruncated :: boolean()}.
get_keys_from_listing(XML) ->
    DocRootElement = get_docroot_from_xml(XML),
    %% Extract only object keys nodes and nothing else using XPath
    XMLKeys = xmerl_xpath:string("/ListBucketResult/Contents/Key/text()",
                                 DocRootElement),
    %% Get listing metadata
    [#xmlText{value=IsTruncated}] =
        xmerl_xpath:string("/ListBucketResult/IsTruncated/text()",
                           DocRootElement),
    %% Get the keys themselves and return
    {ok, [ Key || #xmlText{value=Key} <- XMLKeys ], list_to_atom(IsTruncated)}.

%% ------------------------------------------------------------------
%% @doc
%% Returns a document root from the given XML and removes the namespace
%% attribute in the process.
%%
%% @end
%% ------------------------------------------------------------------
-spec get_docroot_from_xml(XML :: binary()) -> 
    DocRootElement :: #xmlElement{} | #xmlDocument{}.
get_docroot_from_xml(XML) ->
    %% Remove the XMLNS attribute since it complicates our XPath expressions...
    FixedXML =
        re:replace(XML, "xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"", "",
                   [global, multiline, {return, list}]),
    {DocRootElement, _RemainingText = ""} = xmerl_scan:string(FixedXML),
    DocRootElement.

%% ------------------------------------------------------------------
%% @doc
%% Converts XML to something resembling nested proplists. This is an ugly hack
%% that should only be used for very simple XML returns. All values returned are
%% strings.
%%
%% @end
%% ------------------------------------------------------------------
-spec convert_xml(XML :: binary()) ->
    {ok, {string(), string() | list()}} | error().
convert_xml(XML) ->
    %% Remove the XMLNS attribute since it confuses erlsom.
    FixedXML =
        re:replace(XML, "xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"", "",
                   [global, multiline, {return, list}]),
    try erlsom:simple_form(FixedXML) of
        {ok,{Node,_Attrib,Value},_} ->
            {ok, {Node, xml_to_kv(Value)}};
        Error ->
            Error
    catch
        %% Handle exceptions from erlsom when it cannot parse the XML. This
        %% should only happen if S3 returns something bad. We don't really
        %% care about the reason why erlsom failed, but more about the data
        %% returned from S3.
        {error, _Reason} ->
            {error, amazon, XML}
    end.

%% ------------------------------------------------------------------
%% @doc
%% Helper function to remove attributes from XML tags and only keep nodes and
%% values.
%%
%% @end
%% ------------------------------------------------------------------
-spec xml_to_kv(any()) -> [{string(), string() | list()}].
xml_to_kv([{Node,_Attrib,Value}|T]) ->
    [{Node,lists:flatten(xml_to_kv(Value))}|xml_to_kv(T)];
xml_to_kv(Value) ->
    Value.

%% ------------------------------------------------------------------
%% @doc
%% Makes repeated GETs to S3 to create a full object listing. NextMarker
%% describes from which key a listing should continue, set it to "" for the
%% initial call.
%%
%% @end
%% ------------------------------------------------------------------
-spec handle_listing(token(),
                     Bucket :: string(),
                     URL :: string(),
                     NextMarker :: string(),
                     Acc :: [string()]) ->
    {ok, KeyList :: [string()]} | error().
handle_listing(Token = #token{}, Bucket, URL, NextMarker, Acc) ->
    %% The NextMarker should be added differently if there are other
    %% sub-resources already added compared to if the request has no
    %% sub-resources. Therefore we get different cases.
    NewURL = case {NextMarker, lists:last(URL)} of
                 {"", _} -> URL;
                 {Str, $/} -> URL ++ "?marker=" ++ Str;
                 {Str, _Else} -> URL ++ "&marker=" ++ Str
             end,
    case get(Token, Bucket, NewURL, [], "", false) of
        {ok, {{200, _ReasonPhrase}, _Headers, ResponseBody}} ->
            case get_keys_from_listing(ResponseBody) of
                {ok, List, false} ->
                    %% Last response, let's return
                    {ok, lists:append([List|Acc])};
                {ok, List, true} ->
                    %% Get next marker (last element)
                    EndMarker = lists:last(List),
                    handle_listing(Token, Bucket, URL, EndMarker, [List|Acc]);
                Error ->
                    handle_errors(Error)
            end;
        Error ->
            handle_errors(Error)
    end.
    
%% ------------------------------------------------------------------
%% @doc
%% Extracts header tuples from Headers that matches keys in KeyList.
%%
%% @end
%% ------------------------------------------------------------------
-spec extract_headers(Headers :: proplist(), KeyList :: [string()]) ->
    proplist().
extract_headers(Headers, KeyList) ->
    Intersection = 
        gb_sets:to_list(gb_sets:intersection(
                          gb_sets:from_list(proplists:get_keys(Headers)),
                          gb_sets:from_list(KeyList))),
    [ lists:keyfind(Key, 1, Headers) || Key <- Intersection ].

%% ------------------------------------------------------------------
%% @doc
%% Extracts S3 metadata from HTTP headers. Will always return keys in lower
%% case.
%%
%% @end
%% ------------------------------------------------------------------
-spec get_metadata_from_headers(Header :: string()) ->
    proplist().
get_metadata_from_headers(Headers) ->
    FilterFun = fun({"X-Amz-Meta-"++Key, Data}) -> {string:to_lower(Key), Data};
                   (_Else) -> []
                end,
    %% Remove empty lists
    [ {K,V} || {K, V} <- lists:map(FilterFun, Headers) ].

%% ------------------------------------------------------------------
%% @doc
%% Creates Amazon HTTP metadata headers from proplists containing metadata
%% key/value pairs.
%%
%% @end
%% ------------------------------------------------------------------
-spec create_metadata_headers(Metadata :: proplist()) -> proplist().
create_metadata_headers(Metadata) ->
    [ {"x-amz-meta-"++string:to_lower(Key), Value}
      || {Key, Value} <- Metadata ].

%% ------------------------------------------------------------------
%% @doc
%% This function acts as a generic error handler/parser. Use it to handle all
%% exceptional returns from lhttpc.
%%
%% @end
%% ------------------------------------------------------------------
-spec handle_errors({ok, {{StatusCode :: integer(), ReasonPhrase :: string()},
                          Hdrs :: [header()],
                          ResponseBody :: binary() | undefined}} |
                    {error, connection_closed | connect_timeout | timeout}) ->
    error().
handle_errors({client, {{StatusCode, ReasonPhrase}, Headers, Client}}) ->
    %% This case handles errors from hackney when using a client.
    case hackney:body(100000, Client) of
        {ok, Body, Client1} ->
            %% Ok, we've got a body. Reformat the response and call
            %% handle_errors again.
            hackney:close(Client1),
            handle_errors({ok, {{StatusCode, ReasonPhrase},
                                Headers, Body}});
        {error, Reason} ->
            hackney:close(Client),
            handle_errors({error, Reason})
    end;
handle_errors({ok, {{StatusCode, ReasonPhrase}, _Headers, <<>>}}) ->
    %% This case is intended to handle standard HTTP return codes without any
    %% body.
    {error, http, StatusCode, ReasonPhrase};
handle_errors({ok, {{_StatusCode, _ReasonPhrase}, _Headers, ResponseBody}}) ->
    %% This one parses an Amazon S3 error return (XML)
    case convert_xml(ResponseBody) of
        {ok, {"Error", ErrorList}} when is_list(ErrorList) ->
            ErrorCode = proplists:get_value("Code", ErrorList),
            ErrorMsg = proplists:get_value("Message", ErrorList),
            {error, amazon, ErrorCode, ErrorMsg};
        _Error ->
            %% For any other result, treat it as an (other) S3 error and return
            %% the response.
            {error, amazon, ResponseBody}
    end;
handle_errors({error, Reason}) ->
    %% Fall-through for lhttpc errors
    {error, Reason}.

%% ------------------------------------------------------------------
%% @doc
%% Convenience wrapper for making HTTP HEAD requests to S3.
%%
%% @end
%% ------------------------------------------------------------------
head(Token = #token{}, Bucket, Resource, AwsHeaders) ->
    Req = build_request(Token, Bucket, Resource, "HEAD", AwsHeaders, "", "",
                        [], []),
    send_request(Req).

%% ------------------------------------------------------------------
%% @doc
%% Convenience wrapper for making HTTP GET requests to S3.
%%
%% @end
%% ------------------------------------------------------------------
get(Token = #token{}, Bucket, Resource, AwsHeaders, Range, Chunked) ->
    Opts = case Chunked of
               %% The WindowSize is set to ten, based on gut feeling.
               false -> [];
               true -> [{partial_download, [{window_size, 10}]}]
           end,
    Req = build_request(Token, Bucket, Resource, "GET", AwsHeaders, "", Range,
                        [], Opts),
    send_request(Req).
    
%% ------------------------------------------------------------------
%% @doc
%% Convenience wrapper for making HTTP GET requests to S3 using
%% hackney. If Client is called with true, the hackney client will be
%% returned instead of the standard response. The caller is expected to
%% close the connection using hackney:close/1 when done.
%%
%% @end
%% ------------------------------------------------------------------
get_hackney(Token = #token{}, Bucket, Resource, AwsHeaders, Range, Client) ->
    Opts = case Client of
               false -> [];
               true -> [return_client]
           end,
    Req = build_request(Token, Bucket, Resource, "GET", AwsHeaders, "", Range,
                        [], Opts),
    send_hackney_request(Req).

%% ------------------------------------------------------------------
%% @doc
%% Convenience wrapper for making HTTP PUT requests to S3.
%%
%% @end
%% ------------------------------------------------------------------
put(Token = #token{}, Bucket, Resource, AwsHeaders, Body) ->
    ContentType = "application/octet-stream",
    Req = build_request(Token, Bucket, Resource, "PUT", AwsHeaders,
                        ContentType, "", Body, []),
    send_request(Req).

%% ------------------------------------------------------------------
%% @doc
%% Convenience wrapper for making HTTP PUT requests to S3 using
%% hackney (which support sendfile).
%%
%% @end
%% ------------------------------------------------------------------
put_hackney(Token = #token{}, Bucket, Resource, AwsHeaders, Body) ->
    ContentType = "application/octet-stream",
    Req = build_request(Token, Bucket, Resource, "PUT", AwsHeaders,
                        ContentType, "", Body, []),
    send_hackney_request(Req).

%% ------------------------------------------------------------------
%% @doc
%% Convenience wrapper for making HTTP DELETE requests to S3.
%%
%% @end
%% ------------------------------------------------------------------
delete(Token = #token{}, Bucket, Resource, AwsHeaders) ->
    Req = build_request(Token, Bucket, Resource, "DELETE", AwsHeaders, "", "",
                        [], []),
    send_request(Req).

%% ------------------------------------------------------------------
%% @doc
%% Create mandatory HTTP headers, sign those headers using the Amazon S3 REST
%% signing algorithm and send the request using lhttpc.
%%
%% Resource is both the path and the query, such as "/photo.jpg?acl".
%% Options is directly corresponding to the same variable in lhttpc:request/9.
%%
%% @end
%% ------------------------------------------------------------------
-spec build_request(token(),
                    Bucket :: string(),
                    Resource :: string(),
                    Method :: string(),
                    AwsHeaders :: [header()],
                    ContentType :: string(),
                    Range :: string(),
                    Body :: iolist(),
                    Options :: [tuple()]) ->
    RequestRecord :: #request{}.
build_request(#token{host=Host, port=Port, ssl=SSL, keyid=KeyID, secret=Secret,
                     bucket_style=BucketStyle},
              Bucket,
              Resource,
              Method,
              AwsHeaders,
              ContentType,
              Range,
              Body,
              Options) ->
    %% Create some more mandatory AMZ headers
    Date = httpd_util:rfc1123_date(),
    %% Note that x-amz-date overrides the regular date header (which we include
    %% anyway)
    MoreAwsHeaders = AwsHeaders ++ [{"x-amz-date", Date}],
    %% Create signature
    Signature = sign(Bucket,
                     Method,
                     "", %% Content-MD5
                     ContentType,
                     Resource,
                     MoreAwsHeaders,
                     Secret),
    Auth = "AWS " ++ KeyID ++ ":" ++ Signature,
    %% Add headers that wasn't part of signature
    RangeHeader = case Range of
                      "" -> [];
                      Str -> [{"range", Str}]
                  end,
    Hdrs = MoreAwsHeaders ++ RangeHeader ++ [{"date", Date},
                                             {"content-type", ContentType},
                                             {"Authorization", Auth}],
    %% See what bucket access pattern we should use
    {VirtualHost, NewResource} =
        case BucketStyle of
            vhost ->
                %% Use the newer virtual-host-style bucket access
                %% pattern
                {case Bucket of
                     "" -> Host;
                     X -> X ++ "." ++ Host
                 end,
                 Resource};
            path ->
                %% Use old-style path access
                {Host, case Bucket of
                           "" -> Resource;
                           X -> "/" ++ X ++ Resource
                       end}
        end,
    %% Create the request record.
    #request{host=VirtualHost,
             port=Port,
             ssl=SSL,
             resource=NewResource,
             method=Method,
             headers=Hdrs,
             body=Body,
             timeout=?TIMEOUT,
             options=Options}.

%% ------------------------------------------------------------------
%% @doc
%% Send a request extracted from RequestRecord using lhttpc. This function is
%% needed to decopule the actual sending from the building of a request (signing
%% etc).
%%
%% @end
%% ------------------------------------------------------------------
-spec send_request(RequestRecord :: #request{}) ->
    {ok, {{StatusCode :: integer(), ReasonPhrase :: string()},
          Hdrs :: [header()],
          ResponseBody :: binary() | pid() | undefined}} | 
    {error, connection_closed | connect_timeout | timeout}.
send_request(#request{} = Rec) ->
    %% Send our full HTTP request.
    lhttpc:request(Rec#request.host,
                   Rec#request.port,
                   Rec#request.ssl,
                   Rec#request.resource,
                   Rec#request.method,
                   Rec#request.headers,
                   Rec#request.body,
                   Rec#request.timeout,
                   Rec#request.options).

%% ------------------------------------------------------------------
%% @doc
%% Send a request extracted from RequestRecord using hackney. This
%% function is needed to decopule the actual sending from the building
%% of a request (signing etc). Note that the timeout & lhttpc options
%% values are ignored when using hackney. If options contains the atom
%% "return_client", the hackney client will be return instead of the
%% full body. See inline comment below.
%%
%% @end
%% ------------------------------------------------------------------
-spec send_hackney_request(RequestRecord :: #request{}) ->
    {ok | client, {{StatusCode :: integer(), ReasonPhrase :: string()},
                   Hdrs :: [header()],
                   ResponseBody :: binary()}} |
    {error, Reason :: term()}.
send_hackney_request(#request{} = Rec) ->
    Method = list_to_atom(string:to_lower(Rec#request.method)),
    URLProtocol = case Rec#request.ssl of
                      true -> "https://";
                      _Else -> "http://"
                  end,
    URL = iolist_to_binary(URLProtocol ++
                               Rec#request.host ++ ":" ++
                               integer_to_list(Rec#request.port) ++
                               Rec#request.resource),
    Headers = [ {iolist_to_binary(Key), iolist_to_binary(Val)} ||
                  {Key, Val} <- Rec#request.headers ],
    case hackney:request(Method, URL, Headers, Rec#request.body) of
        {ok, StatusCode, RespHeaders, Client} ->
            %% Check if the atom return_client were given in options. If
            %% so, return the client instead of the body. Note that it
            %% is up to the caller to properly close the client using
            %% hackney:close/1.
            case [ X || X <- Rec#request.options, X == return_client ] of
                [_X] ->
                    StrHeaders =
                        [ {binary_to_list(Key), binary_to_list(Val)} ||
                            {Key, Val} <- RespHeaders ],
                    {client,
                     {{StatusCode, "not_given"}, StrHeaders, Client}};
                [] ->
                    {ok, Body, Client1} = hackney:body(Client),
                    hackney:close(Client1),
                    StrHeaders = [ {binary_to_list(Key), binary_to_list(Val)} ||
                                     {Key, Val} <- RespHeaders ],
                    {ok, {{StatusCode, "not_given"}, StrHeaders, Body}}
            end;
        Error ->
            Error
    end.

%% ------------------------------------------------------------------
%% @doc
%% Create a hash string that we can sign for security purposes. The format is
%% specified in the Amazon S3 Dev Guide.
%%
%% @end
%% ------------------------------------------------------------------
-spec sign(Bucket :: string(),
           Method :: string(),
           ContentMD5 :: string(),
           ContentType :: string(),
           Resource :: string(),
           AwsHeaders :: [header()],
           Secret :: string()) ->
    Signature :: string().
sign(Bucket, Method, ContentMD5, ContentType, Resource,
     AwsHeaders, Secret) ->
    %% This construct removes AwsHeaders if it is empty so that the extra
    %% newline char won't get inserted
    Canonicalized = case {canonicalize_headers(AwsHeaders),
                          canonicalize_resource(Bucket, Resource)} of
                        {[], Res} -> Res;
                        {Head, Res} -> Head ++ "\n" ++ Res
                    end,
    HashString = string:join([Method,
                              ContentMD5,
                              ContentType,
                              "", %% Date - we never use it since we send
                                  %% x-amz-date
                              Canonicalized
                             ], "\n"),
    %% Do final header signing
    BinSignature = crypto:sha_mac(Secret, HashString),
    base64:encode_to_string(BinSignature).

%% ------------------------------------------------------------------
%% @doc
%% This function performs canonicalization of a resource according to the Amazon
%% S3 Dev Guide.
%%
%% @end
%% ------------------------------------------------------------------
-spec canonicalize_resource(Bucket :: string(), Resource :: string()) ->
    CanonicalizedResource :: string().
canonicalize_resource(Bucket, Resource) ->
    %% Define sub-resources that should be included in the canonicalized
    %% resource. This list is taken from the S3 documentation.
    FilterList = ["acl", "lifecycle", "location", "logging", "notification",
                  "partNumber", "policy", "requestPayment", "torrent",
                  "uploadId", "uploads", "versionId", "versioning", "versions",
                  "website"],
    %% Extract the resource from any sub-resources and then further split each
    %% sub-resource into key-value pair lists
    [Res|Queries] = re:split(Resource,"[\\?&]",[{return,list}]),
    TokenizedQueries = [ string:tokens(Q, "=") || Q <- Queries ],
    %% Create an intersection between the tokenized sub-resources and the list
    %% of sub-resources that should be included
    FilteredQueries = [ string:join(Query, "=") ||
                          [H|_]=Query <- TokenizedQueries,
                          F <- FilterList,
                          H==F ],
    %% Create the final string and add the "?" separator between resource and
    %% any sub-resources as needed. We also sort the sub-resources since this is
    %% mandated by S3. If no bucket is given, don't add the "/" in front.
    case Bucket of
        "" -> "";
        B -> "/" ++ B
    end ++ Res ++
        case string:join(lists:sort(FilteredQueries), "&") of
            [] -> [];
            Str -> "?" ++ Str
        end.
    
%% ------------------------------------------------------------------
%% @doc
%% This function performs canonicalization of a list of AMZ headers, according
%% to the Amazon S3 Dev Guide.
%%
%% NOTE: Headers should only contain x-amz-* headers. No "regular" HTTP headers
%% are allowed here!
%%
%% NOTE 2: The Amazon canonicalization process is not fully implemented since we
%% do not combine multiple header fields with the same name into a single
%% one. This is because we're in control of the generated headers and therefore
%% trust the developer to not do such stupid things. Morale: don't duplicate
%% header fields.
%%
%% @end
%% ------------------------------------------------------------------
-spec canonicalize_headers(Headers :: [header()]) -> string().
canonicalize_headers(Headers) ->
    CanonHeaders = [ canonicalize_header(Header) || Header <- Headers ],
    SortedHeaders = lists:sort(CanonHeaders),
    string:join(SortedHeaders, "\n").

%% ------------------------------------------------------------------
%% @doc
%% This function performs canonicalization of an AMZ header.
%% Example: {"x-amz-meta-username", " fred,barney"} becomes
%%          "x-amz-meta-username:fred,barney"
%%
%% @end
%% ------------------------------------------------------------------
-spec canonicalize_header(Header :: header()) -> string().
canonicalize_header({Name, Value}) ->
    %% Fix name
    FinalN = string:strip(string:to_lower(Name)),
    %% Fix value (several steps)
    Value2 = string:strip(Value),
    %% Remove any newline characters
    Value3 = re:replace(Value2,"\\n+"," ",[global, multiline, {return, list}]),
    %% Replace multiple spaces with one space
    FinalV = re:replace(Value3,"\\s+"," ",[global, multiline, {return, list}]),
    %% Create final header string
    FinalN++":"++FinalV.
