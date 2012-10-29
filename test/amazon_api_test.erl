%% -*- mode: erlang;erlang-indent-level: 4;indent-tabs-mode: nil -*-
%% ex: ts=4 sw=4 et
%% ===================================================================
%% @copyright 2012 Erik Timan
%% @author Erik Timan <dev@timan.info>
%% ===================================================================
-module(amazon_api_test).

-include_lib("eunit/include/eunit.hrl").

%% This is a simple, basic test of the API that mocks all calls to lhttpc and
%% returns canned expected responses. The test is not complete in any way and
%% error conditions are not tested at all. Its purpose is to serve as a simple
%% functional test of the API.
api_test_() ->
    Token = amazon_api:get_token("triino", 0, true, "73", "ODg="),
    Bucket = "bucket",
    {foreach,
     fun() ->
             meck:new(lhttpc)
     end,
     fun(_) ->
             meck:unload(lhttpc)
     end,
     [{"put_object",
       fun() ->
               meck:expect(lhttpc, request,
                           fun(_Host,
                               _Port,
                               _SSL,
                               "/HejHopp",
                               "PUT",
                               Hdrs,
                               <<"HejHopp">>,
                               _Timeout,
                               _Options) ->
                   %% Ensure metadata is present
                   "stuff" = proplists:get_value("x-amz-meta-mymeta", Hdrs),
                   %% Canned response
                   {ok, {{200, "OK"}, [
                   {"x-amz-id-2", "LriYPLdmOdAiIfgSm/F1YsViT1LW94/xUQxMsF7xiEb1a0wiIOIxl+zbwZ163pt7"},
                   {"x-amz-request-id", "0A49CE4060975EAC"},
                   {"Date", "Wed, 12 Oct 2009 17:50:00 GMT"},
                   {"Etag", "1b2cf535f27731c974343645a3985328"},
                   {"Content-Length", "0"},
                   {"Connection", "close"},
                   {"Server", "AmazonS3"}], <<>>}}
                           end),
               Expected = {ok, [{"Etag", "1b2cf535f27731c974343645a3985328"}]},
               ?assertEqual(Expected,
                            amazon_api:put_object(Token,
                                                  Bucket,
                                                  "HejHopp",
                                                  <<"HejHopp">>,
                                                  [{"mymeta", "stuff"}])),
               ?assert(meck:validate(lhttpc))
       end},
     {"put_bucket",
       fun() ->
               meck:expect(lhttpc, request,
                           fun(Host,
                               _Port,
                               _SSL,
                               "/",
                               "PUT",
                               _Hdrs,
                               _Body,
                               _Timeout,
                               _Options) ->
                   %% Extract bucket name
                   [Bucket|_Tail] = string:tokens(Host, "."),
                   %% Canned response
                   {ok, {{200, "OK"}, [
                   {"x-amz-id-2", "YgIPIfBiKa2bj0KMg95r/0zo3emzU4dzsD4rcKCHQUAdQkf3ShJTOOpXUueF6QKo"},
                   {"x-amz-request-id", "236A8905248E5A01"},
                   {"Date", "Wed, 01 Mar  2009 12:00:00 GMT"},
                   {"Location", "/"++Bucket},
                   {"Content-Length", "0"},
                   {"Connection", "close"},
                   {"Server", "AmazonS3"}], <<>>}}
                           end),
               Expected = ok,
               ?assertEqual(Expected,
                            amazon_api:put_bucket(Token, Bucket, "us-west-2")),
               ?assert(meck:validate(lhttpc))
       end},
      {"list_objects_paginated",
       fun() ->
               meck:expect(lhttpc, request,
                           fun(_Host,
                               _Port,
                               _SSL,
                               Resource,
                               "GET",
                               _Hdrs,
                               [],
                               _Timeout,
                               _Options) ->
                   %% Canned response
                   case Resource of
                       "/" ->
                   {ok, {{200, "OK"}, [
                   {"x-amz-id-2", "gyB+3jRPnrkN98ZajxHXr3u7EFM67bNgSAxexeEHndCX/7GRnfTXxReKUQF28IfP"},
                   {"x-amz-request-id", "3B3C7C725673C630"},
                   {"Date", "Wed, 01 Mar  2009 12:00:00 GMT"},
                   {"Content-Type", "application/xml"},
                   {"Content-Length", "302"},
                   {"Connection", "close"},
                   {"Server", "AmazonS3"}],
                   <<"<?xml version=\"1.0\" encoding=\"UTF-8\"?>
                      <ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01\">
                          <Name>bucket</Name>
                          <Prefix/>
                          <Marker/>
                          <MaxKeys>1</MaxKeys>
                          <IsTruncated>true</IsTruncated>
                          <Contents>
                              <Key>my-image.jpg</Key>
                              <LastModified>2009-10-12T17:50:30.000Z</LastModified>
                              <ETag>&quot;fba9dede5f27731c9771645a39863328&quot;</ETag>
                              <Size>434234</Size>
                              <StorageClass>STANDARD</StorageClass>
                              <Owner>
                                  <ID>75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a</ID>
                                  <DisplayName>mtd@amazon.com</DisplayName>
                              </Owner>
                          </Contents>
                      </ListBucketResult>
                   ">>}};
                       "/?marker=my-image.jpg" ->
                   {ok, {{200, "OK"}, [
                   {"x-amz-id-2", "gyB+3jRPnrkN98ZajxHXr3u7EFM67bNgSAxexeEHndCX/7GRnfTXxReKUQF28IfP"},
                   {"x-amz-request-id", "3B3C7C725673C630"},
                   {"Date", "Wed, 01 Mar  2009 12:00:00 GMT"},
                   {"Content-Type", "application/xml"},
                   {"Content-Length", "302"},
                   {"Connection", "close"},
                   {"Server", "AmazonS3"}],
                   <<"<?xml version=\"1.0\" encoding=\"UTF-8\"?>
                      <ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01\">
                          <Name>bucket</Name>
                          <Prefix/>
                          <Marker>my-image.jpg</Marker>
                          <MaxKeys>1</MaxKeys>
                          <IsTruncated>false</IsTruncated>
                          <Contents>
                             <Key>my-third-image.jpg</Key>
                               <LastModified>2009-10-12T17:50:30.000Z</LastModified>
                              <ETag>&quot;1b2cf535f27731c974343645a3985328&quot;</ETag>
                              <Size>64994</Size>
                              <StorageClass>STANDARD</StorageClass>
                              <Owner>
                                  <ID>75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a</ID>
                                  <DisplayName>mtd@amazon.com</DisplayName>
                              </Owner>
                          </Contents>
                      </ListBucketResult>
                   ">>}}
                   end
                           end),
               %% Here we rely on a certain return order - ugly, should be fixed
               Expected = {ok, ["my-third-image.jpg", "my-image.jpg"]},
               ?assertEqual(Expected,
                            amazon_api:list_objects(Token, Bucket, "")),
               ?assert(meck:validate(lhttpc))
       end},
      {"list_buckets",
       fun() ->
               meck:expect(lhttpc, request,
                           fun(_Host,
                               _Port,
                               _SSL,
                               "/",
                               "GET",
                               _Hdrs,
                               [],
                               _Timeout,
                               _Options) ->
                   %% Canned response
                   {ok, {{200, "OK"}, [
                   {"x-amz-id-2", "gyB+3jRPnrkN98ZajxHXr3u7EFM67bNgSAxexeEHndCX/7GRnfTXxReKUQF28IfP"},
                   {"x-amz-request-id", "3B3C7C725673C630"},
                   {"Date", "Wed, 01 Mar  2009 12:00:00 GMT"},
                   {"Content-Type", "application/xml"},
                   {"Content-Length", "302"},
                   {"Connection", "close"},
                   {"Server", "AmazonS3"}],
                   <<"<?xml version=\"1.0\" encoding=\"UTF-8\"?>
                      <ListAllMyBucketsResult xmlns=\"http://doc.s3.amazonaws.com/2006-03-01\">
                        <Owner>
                          <ID>bcaf1ffd86f461ca5fb16fd081034f</ID>
                          <DisplayName>webfile</DisplayName>
                        </Owner>
                        <Buckets>
                          <Bucket>
                            <Name>quotes</Name>
                            <CreationDate>2006-02-03T16:45:09.000Z</CreationDate>
                          </Bucket>
                          <Bucket>
                            <Name>samples</Name>
                            <CreationDate>2006-02-03T16:41:58.000Z</CreationDate>
                          </Bucket>
                        </Buckets>
                      </ListAllMyBucketsResult>    
                   ">>}}
                           end),
               %% Here we rely on a certain return order - ugly, should be fixed
               Expected = {ok, ["quotes", "samples"]},
               ?assertEqual(Expected, amazon_api:list_buckets(Token)),
               ?assert(meck:validate(lhttpc))
       end},
      {"delete_object",
       fun() ->
               meck:expect(lhttpc, request,
                           fun(_Host,
                               _Port,
                               _SSL,
                               "/HejHopp",
                               "DELETE",
                               _Hdrs,
                               [],
                               _Timeout,
                               _Options) ->
                   %% Canned response
                   {ok, {{204, "NoContent"}, [
                   {"x-amz-id-2", "LriYPLdmOdAiIfgSm/F1YsViT1LW94/xUQxMsF7xiEb1a0wiIOIxl+zbwZ163pt7"},
                   {"x-amz-request-id", "0A49CE4060975EAC"},
                   {"Date", "Wed, 12 Oct 2009 17:50:00 GMT"},
                   {"Etag", "1b2cf535f27731c974343645a3985328"},
                   {"Content-Length", "0"},
                   {"Connection", "close"},
                   {"Server", "AmazonS3"}], <<>>}}
                           end),
               Expected = ok,
               ?assertEqual(Expected,
                            amazon_api:delete_object(Token, Bucket, "HejHopp")),
               ?assert(meck:validate(lhttpc))
       end},
      {"delete_bucket",
       fun() ->
               meck:expect(lhttpc, request,
                           fun(_Host,
                               _Port,
                               _SSL,
                               "/",
                               "DELETE",
                               _Hdrs,
                               [],
                               _Timeout,
                               _Options) ->
                   %% Canned response
                   {ok, {{204, "NoContent"}, [
                   {"x-amz-id-2", "LriYPLdmOdAiIfgSm/F1YsViT1LW94/xUQxMsF7xiEb1a0wiIOIxl+zbwZ163pt7"},
                   {"x-amz-request-id", "0A49CE4060975EAC"},
                   {"Date", "Wed, 12 Oct 2009 17:50:00 GMT"},
                   {"Connection", "close"},
                   {"Server", "AmazonS3"}], <<>>}}
                           end),
               Expected = ok,
               ?assertEqual(Expected,
                            amazon_api:delete_bucket(Token, Bucket)),
               ?assert(meck:validate(lhttpc))
       end},
      {"get_object",
       fun() ->
               meck:expect(lhttpc, request,
                           fun(_Host,
                               _Port,
                               _SSL,
                               "/HejHopp",
                               "GET",
                               _Hdrs,
                               [],
                               _Timeout,
                               _Options) ->
                   %% Canned response
                   {ok, {{200, "OK"}, [
                   {"x-amz-id-2", "eftixk72aD6Ap51TnqcoF8eFidJG9Z/2mkiDFu8yU9AS1ed4OpIszj7UDNEHGran"},
                   {"x-amz-request-id", "x-amz-request-id: 318BC8BC148832E5"},
                   {"X-Amz-Meta-Stuff", "value"},
                   {"Date", "Date: Wed, 28 Oct 2009 22:32:00 GMT"},
                   {"Last-Modified", "Wed, 12 Oct 2009 17:50:00 GMT"},
                   {"Etag", "fba9dede5f27731c9771645a39863328"},
                   {"Content-Length", "10"},
                   {"Content-Type", "application/octet-stream"},
                   {"Connection", "close"},
                   {"Server", "AmazonS3"}], <<"My content">>}}
                           end),
               Expected = {file, <<"My content">>, [{"stuff","value"}],
                           [{"Etag","fba9dede5f27731c9771645a39863328"},
                            {"Last-Modified","Wed, 12 Oct 2009 17:50:00 GMT"}]},
               ?assertEqual(Expected,
                            amazon_api:get_object(Token,
                                                  Bucket,
                                                  "HejHopp",
                                                  false,
                                                  omit)),
               ?assert(meck:validate(lhttpc))
       end},
      {"get_object_metadata",
       fun() ->
               meck:expect(lhttpc, request,
                           fun(_Host,
                               _Port,
                               _SSL,
                               "/HejHopp",
                               "HEAD",
                               _Hdrs,
                               [],
                               _Timeout,
                               _Options) ->
                   %% Canned response
                   {ok, {{200, "OK"}, [
                   {"x-amz-id-2", "eftixk72aD6Ap51TnqcoF8eFidJG9Z/2mkiDFu8yU9AS1ed4OpIszj7UDNEHGran"},
                   {"x-amz-request-id", "x-amz-request-id: 318BC8BC148832E5"},
                   {"X-Amz-Meta-Stuff", "value"},
                   {"Date", "Date: Wed, 28 Oct 2009 22:32:00 GMT"},
                   {"Last-Modified", "Wed, 12 Oct 2009 17:50:00 GMT"},
                   {"Etag", "fba9dede5f27731c9771645a39863328"},
                   {"Content-Length", "10"},
                   {"Content-Type", "application/octet-stream"},
                   {"Connection", "close"},
                   {"Server", "AmazonS3"}], <<>>}}
                           end),
               Expected = {ok, [{"stuff","value"}],
                           [{"Etag","fba9dede5f27731c9771645a39863328"},
                            {"Last-Modified","Wed, 12 Oct 2009 17:50:00 GMT"}]},
               ?assertEqual(Expected,
                            amazon_api:get_object_metadata(Token,
                                                           Bucket,
                                                           "HejHopp")),
               ?assert(meck:validate(lhttpc))
       end}
      ]}.

get_keys_from_listing_test() ->
    XML = <<"<?xml version=\"1.0\" encoding=\"UTF-8\"?>
             <ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01\">
               <Name>bucket</Name>
               <Prefix/>
               <Marker/>
               <MaxKeys>1000</MaxKeys>
               <IsTruncated>false</IsTruncated>
               <Contents>
                   <Key>my-image.jpg</Key>
                   <LastModified>2009-10-12T17:50:30.000Z</LastModified>
                   <ETag>&quot;fba9dede5f27731c9771645a39863328&quot;</ETag>
                   <Size>434234</Size>
                   <StorageClass>STANDARD</StorageClass>
                   <Owner>
                       <ID>75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a</ID>
                       <DisplayName>mtd@amazon.com</DisplayName>
                   </Owner>
               </Contents>
               <Contents>
                  <Key>my-third-image.jpg</Key>
                    <LastModified>2009-10-12T17:50:30.000Z</LastModified>
                   <ETag>&quot;1b2cf535f27731c974343645a3985328&quot;</ETag>
                   <Size>64994</Size>
                   <StorageClass>STANDARD</StorageClass>
                   <Owner>
                       <ID>75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a</ID>
                       <DisplayName>mtd@amazon.com</DisplayName>
                   </Owner>
               </Contents>
           </ListBucketResult>">>,
    Expected = {ok, ["my-image.jpg", "my-third-image.jpg"], false},
    ?assertEqual(Expected, amazon_api:get_keys_from_listing(XML)).

convert_xml_ok_test() ->
    XML = <<"<?xml version=\"1.0\" encoding=\"UTF-8\"?>
             <Error>
               <Code>NoSuchKey</Code>
               <Message>The resource you requested does not exist</Message>
               <Resource>/mybucket/myfoto.jpg</Resource> 
               <RequestId>4442587FB7D0A2F9</RequestId>
             </Error>">>,
    Expected = {ok, {"Error",
                     [{"Code", "NoSuchKey"},
                      {"Message", "The resource you requested does not exist"},
                      {"Resource","/mybucket/myfoto.jpg"},
                      {"RequestId", "4442587FB7D0A2F9"}]}},
    ?assertEqual(Expected, amazon_api:convert_xml(XML)).

convert_xml_error_test() ->
    XML = <<"<?xml version=\"1.0\" encoding=\"UTF-8\"?>
             <Error>
               <Error>
               <RequestId>4442587FB7D0A2F9</RequestId>
             </Error>">>,
    Expected = {error, amazon, XML},
    ?assertEqual(Expected, amazon_api:convert_xml(XML)).

extract_headers_test() ->
    Headers = [{"X-Amz-Interesting-Header1", "noop"},
               {"X-Amz-Interesting-Header2", "noop"},
               {"X-Amz-Uninteresting-Header", "noop"}],
    KeyList = ["X-Amz-Interesting-Header2", "X-Amz-Interesting-Header1"],
    Expected = [{"X-Amz-Interesting-Header1", "noop"},
                {"X-Amz-Interesting-Header2", "noop"}],
    ?assertEqual(Expected, amazon_api:extract_headers(Headers, KeyList)).

get_metadata_from_headers_test() ->
    Headers = [{"X-Amz-Meta-Kallekula", "bokskog"},
               {"X-Amz-Meta-Artemis", "kullersten"},
               {"A-Header-That-Should-Not-Be-Extracted", "no"}],
    Expected = [{"kallekula", "bokskog"}, {"artemis", "kullersten"}],
    ?assertEqual(Expected, amazon_api:get_metadata_from_headers(Headers)).

create_metadata_headers_test() ->
    Metadata = [{"KalleKULA", "bokskog"}, {"artemis", "kullersten"}],
    Expected = [{"x-amz-meta-kallekula", "bokskog"},
                {"x-amz-meta-artemis", "kullersten"}],
    ?assertEqual(Expected, amazon_api:create_metadata_headers(Metadata)).

create_metadata_headers_empty_test() ->
    Expected = [],
    ?assertEqual(Expected, amazon_api:create_metadata_headers([])).

sign_test() ->
    Headers = [{"x-amz-date", "Tue, 27 Mar 2007 21:20:26 +0000"}],
    Key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    %% The commented value below is the value indicated in the S3 documentation,
    %% but our implementation gives a different result. Strangely enough, the
    %% algorithm seems to work as intended in Real Life(TM). Therefore we use
    %% the computed expected value instead (as verified by the S3 Python
    %% bindings).
    %%Expected = "k3nL7gH3+PadhTEVn5Ip83xlYzk=",
    Expected = "R4dJ53KECjStyBO5iTBJZ4XVOaI=",
    Result = amazon_api:sign("johnsmith",
                             "DELETE",
                             "",
                             "",
                             "/photos/puppy.jpg",
                             Headers,
                             Key),
    ?assertEqual(Expected, Result).

canonicalize_resource_test() ->
    Resource = "/test-stuff/bertil?versionId=ffa&prefix=L&acl=true",
    Expected = "/bucket/test-stuff/bertil?acl=true&versionId=ffa",
    Result = amazon_api:canonicalize_resource("bucket", Resource),
    ?assertEqual(Expected, Result).

canonicalize_headers_test() ->
    Input = [{"x-amz-acl", "public-read "},
             {"X-Amz-Meta-ReviewedBy", " joe@johnsmith.net,jane@johnsmith.net"},
             {"X-Amz-Meta-FileChecksum", " 0x02661779"},
             {"X-Amz-Meta-ChecksumAlgorithm", " crc32"}
            ],
    Expected = "x-amz-acl:public-read\nx-amz-meta-checksumalgorithm:crc32\nx-amz-meta-filechecksum:0x02661779\nx-amz-meta-reviewedby:joe@johnsmith.net,jane@johnsmith.net",
    Result = amazon_api:canonicalize_headers(Input),
    ?assertEqual(Expected, Result).

canonicalize_header_test() ->
    BadHeader = {" X-Amz-Meta-UserNAME ", " fred,barney \ntest"},
    Expected = "x-amz-meta-username:fred,barney test",
    Result = amazon_api:canonicalize_header(BadHeader),
    ?assertEqual(Expected, Result).
