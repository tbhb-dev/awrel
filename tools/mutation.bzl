# SPDX-License-Identifier: Apache-2.0
# Copyright Tony Burns

"""Go mutation reports with a platform-specific package exclusion."""

load("@tbhb_rules//quality:defs.bzl", "mutation_gate_test")
load("@tbhb_rules_go//:defs.bzl", "gomutants_report")

def gomutants_test(name, tests, go_mod, go_sum, proxy, data, flags, cpus):
    """Run every package on Unix and exclude persistence on Windows.

    The upstream macro iterates over tests before select is resolved and
    passes compatibility attributes only to the gate. Create the reports
    explicitly so the Windows exclusion also covers the persistence action.

    Args:
        name: Gate target name.
        tests: Go test targets, one per package.
        go_mod: Module file.
        go_sum: Module checksums.
        proxy: Offline module proxy.
        data: Test fixtures.
        flags: Additional gomutants flags.
        cpus: CPUs per report action.
    """
    reports = []
    windows_reports = []
    for test in tests:
        package = native.package_relative_label(test).package
        report = name + "." + package.replace("/", "_")
        is_persist = package == "internal/persist"
        gomutants_report(
            name = report,
            tests = [test],
            go_mod = go_mod,
            go_sum = go_sum,
            proxy = proxy,
            data = data,
            flags = flags,
            cpus = cpus,
            testonly = True,
            target_compatible_with = select({
                "@platforms//os:windows": ["@platforms//:incompatible"],
                "//conditions:default": [],
            }) if is_persist else [],
            visibility = ["//visibility:private"],
        )
        reports.append(":" + report)
        if not is_persist:
            windows_reports.append(":" + report)
    mutation_gate_test(
        name = name,
        reports = select({
            "@platforms//os:windows": windows_reports,
            "//conditions:default": reports,
        }),
    )
