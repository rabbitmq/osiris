%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
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

-define(C_NUM_LOG_FIELDS, 5).

-define(MAGIC, 5).
%% chunk format version
-define(VERSION, 0).
-define(HEADER_SIZE_B, 48).
-define(FILE_OPTS_WRITE, [raw, binary, write, read]).


%% chunk types
-define(CHNK_USER, 0).
-define(CHNK_TRK_DELTA, 1).
-define(CHNK_TRK_SNAPSHOT, 2).

-define(SUP, osiris_server_sup).
