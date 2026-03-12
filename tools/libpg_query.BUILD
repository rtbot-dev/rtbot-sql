config_setting(
    name = "windows",
    constraint_values = ["@platforms//os:windows"],
)

cc_library(
    name = "libpg_query",
    srcs = glob(
        [
            "src/*.c",
            "src/postgres/*.c",
        ],
        exclude = ["src/pg_query_outfuncs_protobuf_cpp.cc"],
    ) + [
        "vendor/protobuf-c/protobuf-c.c",
        "vendor/xxhash/xxhash.c",
        "protobuf/pg_query.pb-c.c",
    ],
    hdrs = [
        "pg_query.h",
        "postgres_deparse.h",
    ],
    copts = select({
        ":windows": [],
        "//conditions:default": [
            "-O3",
            "-fno-strict-aliasing",
            "-fwrapv",
            "-fPIC",
            "-Wno-unused-function",
            "-Wno-unused-value",
            "-Wno-unused-variable",
        ],
    }),
    includes = [
        ".",
        "src/include",
        "src/postgres/include",
        "vendor",
    ] + select({
        ":windows": [
            "src/postgres/include/port/win32",
            "src/postgres/include/port/win32_msvc",
        ],
        "//conditions:default": [],
    }),
    textual_hdrs = glob([
        "src/**/*.h",
        "src/**/*.c",
        "src/postgres/include/**/*.h",
        "vendor/**/*.h",
        "protobuf/**/*.h",
    ]),
    visibility = ["//visibility:public"],
)
