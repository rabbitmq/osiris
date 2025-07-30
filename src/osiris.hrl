%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%

%% logging shim
-define(DEBUG(Fmt, Args), ?DISPATCH_LOG(debug, Fmt, Args)).
-define(DEBUG_IF(Fmt, Args, Bool),
        if Bool ->
               ?DISPATCH_LOG(debug, Fmt, Args);
           true -> ok
        end).
-define(INFO(Fmt, Args), ?DISPATCH_LOG(info, Fmt, Args)).
-define(NOTICE(Fmt, Args), ?DISPATCH_LOG(notice, Fmt, Args)).
-define(WARN(Fmt, Args), ?DISPATCH_LOG(warning, Fmt, Args)).
-define(WARNING(Fmt, Args), ?DISPATCH_LOG(warning, Fmt, Args)).
-define(ERR(Fmt, Args), ?DISPATCH_LOG(error, Fmt, Args)).
-define(ERROR(Fmt, Args), ?DISPATCH_LOG(error, Fmt, Args)).

-define(DISPATCH_LOG(Level, Fmt, Args),
        %% same as OTP logger does when using the macro
        catch (persistent_term:get('$osiris_logger')):log(Level, Fmt, Args,
                                                          #{mfa => {?MODULE,
                                                                    ?FUNCTION_NAME,
                                                                    ?FUNCTION_ARITY},
                                                            file => ?FILE,
                                                            line => ?LINE,
                                                            domain => [osiris]}),
       ok).

-define(IS_STRING(S), is_list(S) orelse is_binary(S)).

-define(C_NUM_LOG_FIELDS, 5).

-define(MAGIC, 5).
%% chunk format version
-define(VERSION, 0).
-define(IDX_VERSION, 1).
-define(LOG_VERSION, 1).
-define(IDX_HEADER, <<"OSII", ?IDX_VERSION:32/unsigned>>).
-define(LOG_HEADER, <<"OSIL", ?LOG_VERSION:32/unsigned>>).
-define(HEADER_SIZE_B, 48).
-define(IDX_HEADER_SIZE, 8).
-define(LOG_HEADER_SIZE, 8).
-define(FILE_OPTS_WRITE, [raw, binary, write, read]).
-define(INDEX_RECORD_SIZE_B, 29).

%% record chunk_info does not map exactly to an index record (field 'num' differs)
-record(chunk_info,
        {id :: osiris:offset(),
         timestamp :: non_neg_integer(),
         epoch :: osiris:epoch(),
         num :: non_neg_integer(),
         type :: osiris_log:chunk_type(),
         %% size of data + filter + trailer
         size :: non_neg_integer(),
         %% position in segment file
         pos :: integer()
        }).
-record(seg_info,
        {file :: file:filename_all(),
         size = 0 :: non_neg_integer(),
         index :: file:filename_all(),
         first :: undefined | #chunk_info{},
         last :: undefined | #chunk_info{}}).

%% chunk types
-define(CHNK_USER, 0).
-define(CHNK_TRK_DELTA, 1).
-define(CHNK_TRK_SNAPSHOT, 2).

-define(SUP, osiris_server_sup).

-define(DEFAULT_FILTER_SIZE, 16).

-define(INFO_(Name, Str, Args),
             ?INFO("~ts [~s:~s/~b] " Str,
                  [Name, ?MODULE, ?FUNCTION_NAME, ?FUNCTION_ARITY | Args])).

-define(WARN_(Name, Str, Args),
             ?WARN("~ts [~s:~s/~b] " Str,
                  [Name, ?MODULE, ?FUNCTION_NAME, ?FUNCTION_ARITY | Args])).

-define(ERROR_(Name, Str, Args),
             ?ERROR("~ts [~s:~s/~b] " Str,
                  [Name, ?MODULE, ?FUNCTION_NAME, ?FUNCTION_ARITY | Args])).

-define(DEBUG_(Name, Str, Args),
             ?DEBUG("~ts [~s:~s/~b] " Str,
                  [Name, ?MODULE, ?FUNCTION_NAME, ?FUNCTION_ARITY | Args])).
