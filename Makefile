PROJECT = osiris
PROJECT_DESCRIPTION = New project
PROJECT_VERSION = 0.1.0

define PROJECT_ENV
[
	{data_dir, "/tmp/osiris"},
	{port_range, {6000, 6500}},
	{max_segment_size_chunks, 256000}
]
endef

LOCAL_DEPS = sasl crypto
dep_gen_batch_server = hex 0.8.6
dep_seshat = git https://github.com/rabbitmq/seshat.git main
DEPS = gen_batch_server seshat

# TEST_DEPS=eunit_formatters looking_glass
dep_tls_gen = git https://github.com/michaelklishin/tls-gen.git master
TEST_DEPS=eunit_formatters tls_gen

dep_looking_glass = git https://github.com/rabbitmq/looking-glass.git master
# PLT_APPS += eunit syntax_tools erts kernel stdlib common_test inets ssh ssl meck looking_glass gen_batch_server inet_tcp_proxy

DIALYZER_OPTS += --src -r test
PLT_APPS += seshat ssl
EUNIT_OPTS = no_tty, {report, {eunit_progress, [colored, profile]}}
include $(if $(ERLANG_MK_FILENAME),$(ERLANG_MK_FILENAME),erlang.mk)

include mk/bazel.mk
