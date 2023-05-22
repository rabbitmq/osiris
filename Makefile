PROJECT = osiris
PROJECT_DESCRIPTION = New project
PROJECT_VERSION = 1.5.1

define PROJECT_ENV
[
	{data_dir, "/tmp/osiris"},
	{port_range, {6000, 6500}},
	{max_segment_size_chunks, 256000},
	{replication_transport, tcp},
	{replica_forced_gc_default_interval, 4999}
]
endef

LOCAL_DEPS = sasl crypto
dep_gen_batch_server = hex 0.8.8
dep_seshat = hex 0.4.0
DEPS = gen_batch_server seshat

dep_looking_glass = git https://github.com/rabbitmq/looking-glass.git master
dep_tls_gen = git https://github.com/rabbitmq/tls-gen.git main
# TEST_DEPS=looking_glass
TEST_DEPS=eunit_formatters tls_gen

DIALYZER_OPTS += -Wunmatched_returns -Werror_handling
PLT_APPS += compiler tools runtime_tools mnesia public_key asn1 ssl inets
EUNIT_OPTS = no_tty, {report, {eunit_progress, [colored, profile]}}
include $(if $(ERLANG_MK_FILENAME),$(ERLANG_MK_FILENAME),erlang.mk)

include mk/bazel.mk
