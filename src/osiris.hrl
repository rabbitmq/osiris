
%% logging shim
-define(DEBUG(Fmt, Args), ?DISPATCH_LOG(debug, Fmt, Args)).
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
                                                            line => ?LINE}),
       ok).

-define(C_NUM_LOG_FIELDS, 3).

-define(MAGIC, 5).
%% chunk format version
-define(VERSION, 0).
-define(HEADER_SIZE_B, 40).
-define(FILE_OPTS_WRITE, [raw, binary, write, read]).


%% chunk types
-define(CHNK_USER, 0).
-define(CHNK_TRK_DELTA, 1).
-define(CHNK_TRK_SNAPSHOT, 2).

