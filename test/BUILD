# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

load("//:bazel/brpc.bzl", "brpc_proto_library")

config_setting(
    name = "darwin",
    values = {"cpu": "darwin"},
    visibility = ["//visibility:public"],
)

COPTS = [
    "-D__STDC_FORMAT_MACROS",
    "-DBTHREAD_USE_FAST_PTHREAD_MUTEX",
    "-D__const__=",
    "-D_GNU_SOURCE",
    "-DUSE_SYMBOLIZE",
    "-DNO_TCMALLOC",
    "-D__STDC_LIMIT_MACROS",
    "-D__STDC_CONSTANT_MACROS",
    "-fPIC",
    "-Wno-unused-parameter",
    "-fno-omit-frame-pointer",
    "-DGFLAGS_NS=google",
    "-Dprivate=public",
    "-Dprotected=public",
    "--include test/sstream_workaround.h",
    "-DBAZEL_TEST=1",
    "-DBVAR_NOT_LINK_DEFAULT_VARIABLES",
    "-DUNIT_TEST",
] + select({
    "//:with_glog": ["-DBRPC_WITH_GLOG=1"],
    "//conditions:default": ["-DBRPC_WITH_GLOG=0"],
})

LINKOPTS = [
    "-lpthread",
    "-lssl",
    "-lcrypto",
    "-ldl",
    "-lz",
] + select({
    ":darwin": [],
    "//conditions:default": [
      "-lrt",

    ],
})

TEST_BUTIL_SOURCES = [
    "at_exit_unittest.cc",
    "atomicops_unittest.cc",
    "base64_unittest.cc",
    "big_endian_unittest.cc",
    "bits_unittest.cc",
    "hash_tables_unittest.cc",
    "linked_list_unittest.cc",
    "mru_cache_unittest.cc",
    "small_map_unittest.cc",
    "stack_container_unittest.cc",
    "cpu_unittest.cc",
    "crash_logging_unittest.cc",
    "leak_tracker_unittest.cc",
    "stack_trace_unittest.cc",
    "environment_unittest.cc",
    "file_util_unittest.cc",
    "dir_reader_posix_unittest.cc",
    "file_path_unittest.cc",
    "file_unittest.cc",
    "scoped_temp_dir_unittest.cc",
    "guid_unittest.cc",
    "hash_unittest.cc",
    "lazy_instance_unittest.cc",
    "aligned_memory_unittest.cc",
    "linked_ptr_unittest.cc",
    "ref_counted_memory_unittest.cc",
    "ref_counted_unittest.cc",
    "scoped_ptr_unittest.cc",
    "scoped_vector_unittest.cc",
    "singleton_unittest.cc",
    "weak_ptr_unittest.cc",
    "observer_list_unittest.cc",
    "file_descriptor_shuffle_unittest.cc",
    "rand_util_unittest.cc",
    "safe_numerics_unittest.cc",
    "scoped_clear_errno_unittest.cc",
    "scoped_generic_unittest.cc",
    "security_unittest.cc",
    "sha1_unittest.cc",
    "stl_util_unittest.cc",
    "nullable_string16_unittest.cc",
    "safe_sprintf_unittest.cc",
    "string16_unittest.cc",
    "stringprintf_unittest.cc",
    "string_number_conversions_unittest.cc",
    "string_piece_unittest.cc",
    "string_split_unittest.cc",
    "string_tokenizer_unittest.cc",
    "string_util_unittest.cc",
    "stringize_macros_unittest.cc",
    "sys_string_conversions_unittest.cc",
    "utf_offset_string_conversions_unittest.cc",
    "utf_string_conversions_unittest.cc",
    "cancellation_flag_unittest.cc",
    "condition_variable_unittest.cc",
    "lock_unittest.cc",
    "waitable_event_unittest.cc",
    "type_traits_unittest.cc",
    "non_thread_safe_unittest.cc",
    "platform_thread_unittest.cc",
    "simple_thread_unittest.cc",
    "thread_checker_unittest.cc",
    "thread_collision_warner_unittest.cc",
    "thread_id_name_manager_unittest.cc",
    "thread_local_storage_unittest.cc",
    "thread_local_unittest.cc",
    "watchdog_unittest.cc",
    "time_unittest.cc",
    "version_unittest.cc",
    "logging_unittest.cc",
    "cacheline_unittest.cpp",
    "class_name_unittest.cpp",
    "endpoint_unittest.cpp",
    "unique_ptr_unittest.cpp",
    "errno_unittest.cpp",
    "fd_guard_unittest.cpp",
    "file_watcher_unittest.cpp",
    "find_cstr_unittest.cpp",
    "scoped_lock_unittest.cpp",
    "status_unittest.cpp",
    "string_printf_unittest.cpp",
    "string_splitter_unittest.cpp",
    "synchronous_event_unittest.cpp",
    "temp_file_unittest.cpp",
    "baidu_thread_local_unittest.cpp",
    "baidu_time_unittest.cpp",
    "flat_map_unittest.cpp",
    "crc32c_unittest.cc",
    "iobuf_unittest.cpp",
    "object_pool_unittest.cpp",
    "test_switches.cc",
    "scoped_locale.cc",
    "recordio_unittest.cpp",
    #"popen_unittest.cpp",
    "bounded_queue_unittest.cc",
    "butil_unittest_main.cpp",
] + select({
        "@bazel_tools//tools/osx:darwin": [],
        "//conditions:default": [
           "test_file_util_linux.cc",
           "proc_maps_linux_unittest.cc",
        ],
})

proto_library(
    name = "test_proto",
    srcs = glob([
        "*.proto",
      ],
      exclude = [
          "echo.proto",
      ]
    ),
    visibility = ["//visibility:public"],
)

cc_proto_library(
    name = "cc_test_proto",
    deps = [
        ":test_proto",
    ],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "sstream_workaround",
    hdrs = [
        "sstream_workaround.h",
    ]
)

cc_test(
    name = "butil_test",
    srcs = TEST_BUTIL_SOURCES + [
        "scoped_locale.h",
        "multiprocess_func_list.h",
        "test_switches.h",
    ],
    deps = [
        ":sstream_workaround",
        ":cc_test_proto",
        "//:brpc",
        "@com_google_googletest//:gtest",
    ],
    copts = COPTS,
)

cc_test(
    name = "bvar_test",
    srcs = glob([
        "bvar_*_unittest.cpp",
    ],
    exclude = [
        "bvar_lock_timer_unittest.cpp",
        "bvar_recorder_unittest.cpp",
    ]),
    deps = [
        ":sstream_workaround",
        "//:bvar",
        "@com_google_googletest//:gtest",
    ],
    copts = COPTS,
)

cc_test(
    name = "bthread_test",
    srcs = glob([
        "bthread_*_unittest.cpp",
    ],
    exclude = [
        "bthread_cond_unittest.cpp",
        "bthread_execution_queue_unittest.cpp",
        "bthread_dispatcher_unittest.cpp",
        "bthread_fd_unittest.cpp",
        "bthread_mutex_unittest.cpp",
        "bthread_brpc_rwlock_unittest.cpp",
        "bthread_setconcurrency_unittest.cpp",
        # glog CHECK die with a fatal error
        "bthread_key_unittest.cpp"
    ]),
    deps = [
        ":sstream_workaround",
        "//:brpc",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
    ],
    copts = COPTS,
)

