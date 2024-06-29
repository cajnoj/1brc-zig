const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});

    const exe = b.addExecutable(.{ .name = "z1brc", .root_source_file = b.path("src/main.zig"), .target = target, .optimize = .ReleaseFast, .strip = true });

    b.installArtifact(exe);
}
