PROJECT = osiris
PROJECT_DESCRIPTION = New project
PROJECT_VERSION = 0.1.0

define PROJECT_ENV
[
	{data_dir, "/tmp/osiris"},
	{port_range, {6000, 6500}}
]
endef

LOCAL_DEPS = sasl crypto
dep_gen_batch_server = hex 0.8.2
DEPS = gen_batch_server

TEST_DEPS=eunit_formatters looking_glass

dep_looking_glass = git https://github.com/rabbitmq/looking-glass.git master
# PLT_APPS += eunit syntax_tools erts kernel stdlib common_test inets ssh ssl meck looking_glass gen_batch_server inet_tcp_proxy

DIALYZER_OPTS += --src -r test
EUNIT_OPTS = no_tty, {report, {eunit_progress, [colored, profile]}}
include $(if $(ERLANG_MK_FILENAME),$(ERLANG_MK_FILENAME),erlang.mk)
