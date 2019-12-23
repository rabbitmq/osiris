PROJECT = osiris
PROJECT_DESCRIPTION = New project
PROJECT_VERSION = 0.1.0

LOCAL_DEPS = sasl crypto
dep_gen_batch_server = hex 0.8.2
DEPS = gen_batch_server

PLT_APPS += eunit proper syntax_tools erts kernel stdlib common_test inets aten mnesia ssh ssl meck looking_glass gen_batch_server inet_tcp_proxy

DIALYZER_OPTS += --src -r test
EUNIT_OPTS = no_tty, {report, {eunit_progress, [colored, profile]}}
include $(if $(ERLANG_MK_FILENAME),$(ERLANG_MK_FILENAME),erlang.mk)
