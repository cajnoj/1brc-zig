const std = @import("std");
const FloatMode = std.builtin.FloatMode;
const Thread = std.Thread;
const Mutex = std.Thread.Mutex;
const math = std.math;

const read_buf_size: usize = 1024 * 1024;
const file_name = "measurements.txt";

const vector_size: usize = std.simd.suggestVectorLength(f32).?;

const Measurements = struct {
    const max_value: f32 = 99.9;
    const min_value: f32 = -99.9;

    min: f32 = max_value,
    max: f32 = min_value,
    sum: f32 = 0,
    num: u32 = 0,

    buffer: @Vector(vector_size, f32) = undefined,

    fn add(self: *Measurements, temp: f32) void {
        @setRuntimeSafety(false);
        @setFloatMode(FloatMode.optimized);

        const mod = @mod(self.num, vector_size);
        self.num += 1;
        self.buffer[mod] = temp;

        // Notice that mod was computed before the vetcor assignment (for performance reasons)
        if (mod == 0) {
            self.min = @min(self.min, @reduce(.Min, self.buffer));
            self.max = @max(self.max, @reduce(.Max, self.buffer));
            self.sum += @reduce(.Add, self.buffer);
        }
    }

    fn close(self: *Measurements) void {
        if (self.num % vector_size != 0) {
            for (0..@mod(self.num, vector_size)) |i| {
                self.min = @min(self.min, self.buffer[i]);
                self.max = @max(self.max, self.buffer[i]);
                self.sum += self.buffer[i];
            }
        }
    }
};

fn stringLessThan(what: void, lhs: []const u8, rhs: []const u8) bool {
    _ = what;
    return std.mem.lessThan(u8, lhs, rhs);
}

var agg_mutex = Mutex{};

fn MyHashMap() type {
    return std.HashMap([]const u8, Measurements, std.hash_map.StringContext, std.hash_map.default_max_load_percentage);
}

const Map = struct {
    data: MyHashMap(),
    allocator: std.mem.Allocator,
    id: ?usize,

    fn init(allocator: std.mem.Allocator, id: ?usize) Map {
        return Map{
            .data = MyHashMap().init(allocator),
            .allocator = allocator,
            .id = id,
        };
    }

    fn deinit(map: *Map) void {
        var iterator = map.data.iterator();
        while (iterator.next()) |entry| {
            map.allocator.free(entry.key_ptr.*);
        }
        map.data.deinit();
    }

    fn sortedKeys(map: Map) ![][]const u8 {
        var keys_list = std.ArrayList([]const u8).init(map.allocator);
        errdefer keys_list.deinit();

        var key_iterator = map.data.keyIterator();
        while (key_iterator.next()) |key| {
            try keys_list.append(key.*);
        }

        const keys_slice = try keys_list.toOwnedSlice(); // deinits array list
        std.mem.sort([]const u8, keys_slice, {}, stringLessThan);

        return keys_slice;
    }

    fn print(map: Map) !void {
        const std_out = std.io.getStdOut();
        const out = std_out.writer();

        const keys = try map.sortedKeys();
        defer map.allocator.free(keys);

        try out.print("{{", .{});
        for (keys, 0..) |key, i| {
            const val = map.data.get(key).?;
            try out.print("{s}{s}={d:.1}/{d:.1}/{d:.1}", .{
                if (i > 0) ", " else "",
                key,
                val.min,
                val.sum / @as(f32, @floatFromInt(val.num)),
                val.max,
            });
        }
        try out.print("}}\n", .{});
    }

    fn put(map: *Map, city: []const u8, temp: f32) !void {
        @setRuntimeSafety(false);
        @setFloatMode(FloatMode.optimized);

        if (map.data.getPtr(city)) |valptr| {
            valptr.add(temp);
        } else {
            const key = try map.allocator.dupe(u8, city);
            var val = Measurements{};
            val.add(temp);
            try map.data.put(key, val);
        }
    }

    fn apply(self: *Map, other: *Map) !void {
        @setRuntimeSafety(false);
        @setFloatMode(FloatMode.optimized);

        var other_iterator = other.data.iterator();
        while (other_iterator.next()) |other_item| {
            const other_city = other_item.key_ptr.*;
            var other_val = other_item.value_ptr.*;

            other_val.close();

            if (self.data.getPtr(other_city)) |self_val_ptr| {
                if (other_val.min < self_val_ptr.min) self_val_ptr.min = other_val.min;

                if (other_val.max > self_val_ptr.max) self_val_ptr.max = other_val.max;

                self_val_ptr.num += other_val.num;
                self_val_ptr.sum += other_val.sum;
            } else {
                const key = try self.allocator.dupe(u8, other_city);
                try self.data.put(key, other_val);
            }
        }
    }
};

pub fn process(offset_from: u64, max_bytes: u64, aggmap: *Map, id: usize) !void {
    @setRuntimeSafety(false);
    @setFloatMode(FloatMode.optimized);

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var map = Map.init(allocator, id);
    defer map.deinit();

    const file = try std.fs.cwd().openFile(file_name, .{ .mode = .read_only, .lock = .shared });
    try file.seekTo(offset_from);
    defer file.close();

    var buf_reader = std.io.bufferedReaderSize(read_buf_size, file.reader());
    var reader = buf_reader.reader();
    var city_buf: [100]u8 = undefined;
    var read_bytes: u64 = 0;

    if (id > 0) {
        if (try reader.readUntilDelimiterOrEof(&city_buf, '\n')) |skipped| {
            read_bytes += skipped.len;
        }
    }

    while (read_bytes < max_bytes) {
        if (try reader.readUntilDelimiterOrEof(&city_buf, ';')) |city| {
            read_bytes += city.len;
            const temp = try readF32(reader, &read_bytes);
            try map.put(city, temp);
        } else break;
    }

    agg_mutex.lock();
    defer agg_mutex.unlock();

    try aggmap.apply(&map);
}

fn getFileSize() !u64 {
    const file = try std.fs.cwd().openFile(file_name, .{ .mode = .read_only, .lock = .shared });
    defer file.close();

    const file_stats = try file.stat();

    return file_stats.size;
}

pub fn main() !void {
    const file_size = try getFileSize();
    const num_threads: usize = try Thread.getCpuCount();
    const thread_bytes: u64 = file_size / num_threads;

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var aggmap = Map.init(allocator, null);
    defer aggmap.deinit();

    var threads: []Thread = try allocator.alloc(Thread, num_threads);
    defer allocator.free(threads);

    for (0..num_threads) |i| {
        const offset: u64 = file_size * @as(u64, i) / @as(u64, num_threads);
        threads[i] = try Thread.spawn(.{}, process, .{ if (i == 0) 0 else offset - 1, thread_bytes, &aggmap, i });
    }

    agg_mutex.lock();
    agg_mutex.unlock();

    for (threads) |thread| {
        defer thread.join();
    }

    try aggmap.print();
}

pub fn readF32(reader: anytype, bytes_counter: *u64) !f32 {
    @setRuntimeSafety(false);
    @setFloatMode(FloatMode.optimized);

    var c = try reader.readByte();
    const negative = c == '-';
    var int_value: u32 = undefined;

    if (negative) {
        c = try reader.readByte();
        int_value = c - '0';
        c = try reader.readByte();
        if (c != '.') {
            int_value *= 10;
            int_value += c - '0';
            bytes_counter.* += @as(u64, 6);
            c = try reader.readByte();
            std.debug.assert(c == '.');
        } else bytes_counter.* += @as(u64, 5);
    } else {
        int_value = c - '0';
        c = try reader.readByte();
        if (c != '.') {
            int_value *= 10;
            int_value += c - '0';
            bytes_counter.* += @as(u64, 5);
            c = try reader.readByte();
            std.debug.assert(c == '.');
        } else bytes_counter.* += @as(u64, 4);
    }

    // Read fraction
    c = try reader.readByte();

    const whole_part: f32 = @as(f32, @floatFromInt(int_value));
    const fraction: f32 = @as(f32, @floatFromInt(c - '0')) / 10;
    const value = whole_part + fraction;

    // Skip EOL
    c = try reader.readByte();
    std.debug.assert(c == '\n');

    if (negative) {
        return -value;
    }

    return value;
}
