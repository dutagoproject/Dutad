import ctypes as C
import json
import os
import sys
import time
from pathlib import Path


CL_SUCCESS = 0
CL_DEVICE_TYPE_GPU = 1 << 2
CL_MEM_READ_ONLY = 1 << 2
CL_MEM_WRITE_ONLY = 1 << 1
CL_MEM_READ_WRITE = 1 << 0
CL_TRUE = 1
CL_PROGRAM_BUILD_LOG = 0x1183
CL_DEVICE_NAME = 0x102B
CL_PLATFORM_NAME = 0x0902


KERNEL_SRC = r"""
__constant ulong RC[24] = {
    0x0000000000000001UL, 0x0000000000008082UL,
    0x800000000000808aUL, 0x8000000080008000UL,
    0x000000000000808bUL, 0x0000000080000001UL,
    0x8000000080008081UL, 0x8000000000008009UL,
    0x000000000000008aUL, 0x0000000000000088UL,
    0x0000000080008009UL, 0x000000008000000aUL,
    0x000000008000808bUL, 0x800000000000008bUL,
    0x8000000000008089UL, 0x8000000000008003UL,
    0x8000000000008002UL, 0x8000000000000080UL,
    0x000000000000800aUL, 0x800000008000000aUL,
    0x8000000080008081UL, 0x8000000000008080UL,
    0x0000000080000001UL, 0x8000000080008008UL
};

inline ulong rol64(ulong x, uint r) { return rotate(x, (ulong)r); }

inline ulong load64_le(const __global uchar* p) {
    ulong v = 0;
    for (uint i = 0; i < 8; i++) v |= ((ulong)p[i]) << (8 * i);
    return v;
}

inline ulong load64_private(const uchar* p) {
    ulong v = 0;
    for (uint i = 0; i < 8; i++) v |= ((ulong)p[i]) << (8 * i);
    return v;
}

inline void store64_private(uchar* p, ulong v) {
    for (uint i = 0; i < 8; i++) p[i] = (uchar)((v >> (8 * i)) & 0xffUL);
}

inline ulong mix_u64(ulong a, ulong b, ulong c) {
    ulong z = (a + rol64(b ^ c, 19)) * 0x9E3779B97F4A7C15UL;
    ulong y = z ^ rol64(a + c, 27);
    return y + rol64(b, 41);
}

void keccakf(ulong st[25]) {
    const uint rho[24] = {
        1, 3, 6, 10, 15, 21, 28, 36, 45, 55, 2, 14,
        27, 41, 56, 8, 25, 43, 62, 18, 39, 61, 20, 44
    };
    const uint pi[24] = {
        10, 7, 11, 17, 18, 3, 5, 16, 8, 21, 24, 4,
        15, 23, 19, 13, 12, 2, 20, 14, 22, 9, 6, 1
    };
    for (uint round = 0; round < 24; round++) {
        ulong bc[5];
        for (uint i = 0; i < 5; i++) {
            bc[i] = st[i] ^ st[i + 5] ^ st[i + 10] ^ st[i + 15] ^ st[i + 20];
        }
        for (uint i = 0; i < 5; i++) {
            ulong t = bc[(i + 4) % 5] ^ rol64(bc[(i + 1) % 5], 1);
            for (uint j = 0; j < 25; j += 5) st[j + i] ^= t;
        }
        ulong t = st[1];
        for (uint i = 0; i < 24; i++) {
            uint j = pi[i];
            ulong tmp = st[j];
            st[j] = rol64(t, rho[i]);
            t = tmp;
        }
        for (uint j = 0; j < 25; j += 5) {
            ulong a0 = st[j + 0];
            ulong a1 = st[j + 1];
            ulong a2 = st[j + 2];
            ulong a3 = st[j + 3];
            ulong a4 = st[j + 4];
            st[j + 0] = a0 ^ ((~a1) & a2);
            st[j + 1] = a1 ^ ((~a2) & a3);
            st[j + 2] = a2 ^ ((~a3) & a4);
            st[j + 3] = a3 ^ ((~a4) & a0);
            st[j + 4] = a4 ^ ((~a0) & a1);
        }
        st[0] ^= RC[round];
    }
}

void sha3_256_bytes(const uchar* in, uint len, uchar out[32]) {
    ulong st[25];
    for (uint i = 0; i < 25; i++) st[i] = 0;
    uint rate = 136;
    uint off = 0;
    while (len >= rate) {
        for (uint i = 0; i < rate / 8; i++) {
            st[i] ^= load64_private(in + off + i * 8);
        }
        keccakf(st);
        off += rate;
        len -= rate;
    }
    uchar block[136];
    for (uint i = 0; i < rate; i++) block[i] = 0;
    for (uint i = 0; i < len; i++) block[i] = in[off + i];
    block[len] = 0x06;
    block[rate - 1] |= 0x80;
    for (uint i = 0; i < rate / 8; i++) st[i] ^= load64_private(block + i * 8);
    keccakf(st);
    for (uint i = 0; i < 4; i++) store64_private(out + i * 8, st[i]);
}

__kernel void bench_v41(
    __global const uchar* dataset,
    uint nblocks,
    __global ulong* scratch_all,
    uint scratch_words,
    __global const uchar* header80,
    ulong height,
    __global const uchar* anchor32,
    ulong nonce_base,
    uint hashes_per_item,
    __global uchar* out_digest
) {
    const uint gid = get_global_id(0);
    __global ulong* scratch = scratch_all + ((ulong)gid * (ulong)scratch_words);
    ulong nonce = nonce_base + ((ulong)gid * (ulong)hashes_per_item);
    uchar seed_in[160];
    uchar seed[32];
    uchar seed_block[32];
    uchar next_in[48];
    uchar fin_in[224];
    uchar out[32];

    for (uint iter = 0; iter < hashes_per_item; iter++, nonce++) {
        uint pos = 0;
        const char tag1[15] = {'C','P','U','C','O','I','N','_','P','O','W','_','V','4','|'};
        for (uint i = 0; i < 15; i++) seed_in[pos++] = (uchar)tag1[i];
        for (uint i = 0; i < 80; i++) seed_in[pos++] = header80[i];
        store64_private(seed_in + pos, nonce); pos += 8;
        store64_private(seed_in + pos, height); pos += 8;
        store64_private(seed_in + pos, height / 2048UL); pos += 8;
        for (uint i = 0; i < 32; i++) seed_in[pos++] = anchor32[i];
        sha3_256_bytes(seed_in, pos, seed);

        ulong s0 = load64_private(seed + 0);
        ulong s1 = load64_private(seed + 8);
        ulong s2 = load64_private(seed + 16);
        ulong s3 = load64_private(seed + 24);
        ulong lane = s0 ^ s2;

        pos = 0;
        const char tag2[19] = {'C','P','U','C','O','I','N','_','P','O','W','_','V','4','_','S','C','R','|'};
        for (uint i = 0; i < 19; i++) seed_in[pos++] = (uchar)tag2[i];
        for (uint i = 0; i < 80; i++) seed_in[pos++] = header80[i];
        store64_private(seed_in + pos, nonce); pos += 8;
        store64_private(seed_in + pos, height); pos += 8;
        for (uint i = 0; i < 32; i++) seed_in[pos++] = anchor32[i];
        sha3_256_bytes(seed_in, pos, seed_block);

        for (uint i = 0; i < scratch_words; i++) {
            if ((i & 3U) == 0U) {
                for (uint j = 0; j < 32; j++) next_in[j] = seed_block[j];
                store64_private(next_in + 32, (ulong)i);
                store64_private(next_in + 40, height / 2048UL);
                sha3_256_bytes(next_in, 48, seed_block);
            }
            uint off = (i & 3U) * 8U;
            scratch[i] = load64_private(seed_block + off) ^ ((ulong)i * 0x9E3779B9UL);
        }

        for (ulong t = 0; t < 4096UL; t++) {
            ulong idx0 = lane ^ rol64(s1, 11) ^ (t * 0x9E3779B97F4A7C15UL);
            uint j0 = (uint)(idx0 % (ulong)nblocks);
            uint off0 = j0 * 64U;
            ulong x0 = load64_le(dataset + off0 + 0);
            ulong x1 = load64_le(dataset + off0 + 16);

            ulong idx1 = s2 ^ rol64(s3, 29) ^ x0 ^ x1 ^ (lane * 0xD1B54A32D192ED03UL);
            uint j1 = (uint)(idx1 % (ulong)nblocks);
            uint off1 = j1 * 64U;
            ulong y0 = load64_le(dataset + off1 + 8);
            ulong y1 = load64_le(dataset + off1 + 24);

            uint sp_idx0 = (uint)((lane ^ s0 ^ x0) % (ulong)scratch_words);
            uint sp_idx1 = (uint)((rol64(s1, 9) ^ y0 ^ t) % (ulong)scratch_words);
            uint sp_idx2 = (uint)((rol64(s2, 17) ^ x1 ^ lane) % (ulong)scratch_words);
            uint sp_idx3 = (uint)((rol64(s3, 27) ^ y1 ^ s0) % (ulong)scratch_words);

            ulong sp0 = scratch[sp_idx0];
            ulong sp1 = scratch[sp_idx1];
            ulong sp2 = scratch[sp_idx2];
            ulong sp3 = scratch[sp_idx3];

            ulong extra = 0;
            for (uint r = 0; r < 4; r++) {
                ulong mix = r == 0 ? (lane ^ sp0 ^ s2)
                           : r == 1 ? (s0 ^ sp1 ^ y1)
                           : r == 2 ? (s1 ^ sp2 ^ x0)
                                    : (s3 ^ sp3 ^ x1);
                ulong idx = mix ^ ((t + (ulong)r) * 0x94D049BB133111EBUL);
                uint j = (uint)(idx % (ulong)nblocks);
                uint off = j * 64U;
                ulong lo = load64_le(dataset + off + 0);
                ulong hi = load64_le(dataset + off + 32);
                extra ^= mix_u64(lo, hi, mix ^ (ulong)r);
            }

            lane = mix_u64(lane ^ x0 ^ sp0, y1 ^ sp1, t ^ s3 ^ extra);
            s0 = mix_u64(s0 ^ x0 ^ extra, y0 ^ sp2, lane);
            s1 = mix_u64(s1 ^ sp1, x1 ^ lane ^ extra, s0);
            s2 = mix_u64(s2 ^ y0 ^ sp3, s0 ^ extra, x1);
            s3 = mix_u64(s3 ^ y1 ^ extra, s1 ^ sp0, lane);

            scratch[sp_idx0] = mix_u64(sp0 ^ x0, lane, extra);
            scratch[sp_idx1] = mix_u64(sp1 ^ y0, s0, lane);
            scratch[sp_idx2] = mix_u64(sp2 ^ x1, s1, extra);
            scratch[sp_idx3] = mix_u64(sp3 ^ y1, s2, s3);
        }

        uint fin = 0;
        const char tag3[19] = {'C','P','U','C','O','I','N','_','P','O','W','_','V','4','_','F','I','N','|'};
        for (uint i = 0; i < 19; i++) fin_in[fin++] = (uchar)tag3[i];
        for (uint i = 0; i < 80; i++) fin_in[fin++] = header80[i];
        store64_private(fin_in + fin, nonce); fin += 8;
        store64_private(fin_in + fin, height); fin += 8;
        store64_private(fin_in + fin, height / 2048UL); fin += 8;
        for (uint i = 0; i < 32; i++) fin_in[fin++] = anchor32[i];
        store64_private(fin_in + fin, lane); fin += 8;
        store64_private(fin_in + fin, s0); fin += 8;
        store64_private(fin_in + fin, s1); fin += 8;
        store64_private(fin_in + fin, s2); fin += 8;
        store64_private(fin_in + fin, s3); fin += 8;
        store64_private(fin_in + fin, scratch[0]); fin += 8;
        store64_private(fin_in + fin, scratch[scratch_words / 2]); fin += 8;
        store64_private(fin_in + fin, scratch[scratch_words - 1]); fin += 8;
        sha3_256_bytes(fin_in, fin, out);
    }
    for (uint i = 0; i < 32; i++) out_digest[gid * 32 + i] = out[i];
}
"""


def check(err, msg):
    if err != CL_SUCCESS:
        raise RuntimeError(f"{msg}: OpenCL error {err}")


def get_str(func, obj, param):
    size = C.c_size_t()
    check(func(obj, param, 0, None, C.byref(size)), f"query {param}")
    buf = (C.c_char * size.value)()
    check(func(obj, param, size, buf, None), f"read {param}")
    return bytes(buf).rstrip(b"\0").decode(errors="ignore")


def get_build_log(cl, program, device):
    size = C.c_size_t()
    check(cl.clGetProgramBuildInfo(program, device, CL_PROGRAM_BUILD_LOG, 0, None, C.byref(size)), "query build log")
    buf = (C.c_char * size.value)()
    check(cl.clGetProgramBuildInfo(program, device, CL_PROGRAM_BUILD_LOG, size, buf, None), "read build log")
    return bytes(buf).rstrip(b"\0").decode(errors="ignore")


def main():
    out_dir = Path(sys.argv[1] if len(sys.argv) > 1 else r"C:\FINAL RELEASE\Mandatory\dutad\target\bench_v41")
    global_work_size = int(sys.argv[2]) if len(sys.argv) > 2 else 16
    hashes_per_item = int(sys.argv[3]) if len(sys.argv) > 3 else 2
    dataset = (out_dir / "dataset.bin").read_bytes()
    header = (out_dir / "header80.bin").read_bytes()
    anchor = (out_dir / "anchor32.bin").read_bytes()
    cpu_report = json.loads((out_dir / "cpu_report.json").read_text())

    cl = C.WinDLL("OpenCL.dll")
    cl.clGetPlatformIDs.argtypes = [C.c_uint, C.c_void_p, C.POINTER(C.c_uint)]
    cl.clGetDeviceIDs.argtypes = [C.c_void_p, C.c_ulong, C.c_uint, C.c_void_p, C.POINTER(C.c_uint)]
    cl.clGetPlatformInfo.argtypes = [C.c_void_p, C.c_uint, C.c_size_t, C.c_void_p, C.POINTER(C.c_size_t)]
    cl.clGetDeviceInfo.argtypes = [C.c_void_p, C.c_uint, C.c_size_t, C.c_void_p, C.POINTER(C.c_size_t)]
    cl.clGetProgramBuildInfo.argtypes = [C.c_void_p, C.c_void_p, C.c_uint, C.c_size_t, C.c_void_p, C.POINTER(C.c_size_t)]
    cl.clCreateContext.argtypes = [C.c_void_p, C.c_uint, C.POINTER(C.c_void_p), C.c_void_p, C.c_void_p, C.POINTER(C.c_int)]
    cl.clCreateContext.restype = C.c_void_p
    cl.clCreateCommandQueue.argtypes = [C.c_void_p, C.c_void_p, C.c_ulong, C.POINTER(C.c_int)]
    cl.clCreateCommandQueue.restype = C.c_void_p
    cl.clCreateProgramWithSource.argtypes = [C.c_void_p, C.c_uint, C.POINTER(C.c_char_p), C.POINTER(C.c_size_t), C.POINTER(C.c_int)]
    cl.clCreateProgramWithSource.restype = C.c_void_p
    cl.clBuildProgram.argtypes = [C.c_void_p, C.c_uint, C.POINTER(C.c_void_p), C.c_char_p, C.c_void_p, C.c_void_p]
    cl.clCreateKernel.argtypes = [C.c_void_p, C.c_char_p, C.POINTER(C.c_int)]
    cl.clCreateKernel.restype = C.c_void_p
    cl.clCreateBuffer.argtypes = [C.c_void_p, C.c_ulong, C.c_size_t, C.c_void_p, C.POINTER(C.c_int)]
    cl.clCreateBuffer.restype = C.c_void_p
    cl.clEnqueueWriteBuffer.argtypes = [C.c_void_p, C.c_void_p, C.c_uint, C.c_size_t, C.c_size_t, C.c_void_p, C.c_uint, C.c_void_p, C.c_void_p]
    cl.clEnqueueReadBuffer.argtypes = [C.c_void_p, C.c_void_p, C.c_uint, C.c_size_t, C.c_size_t, C.c_void_p, C.c_uint, C.c_void_p, C.c_void_p]
    cl.clSetKernelArg.argtypes = [C.c_void_p, C.c_uint, C.c_size_t, C.c_void_p]
    cl.clEnqueueNDRangeKernel.argtypes = [C.c_void_p, C.c_void_p, C.c_uint, C.c_void_p, C.POINTER(C.c_size_t), C.c_void_p, C.c_uint, C.c_void_p, C.c_void_p]
    cl.clFinish.argtypes = [C.c_void_p]

    num_platforms = C.c_uint()
    check(cl.clGetPlatformIDs(0, None, C.byref(num_platforms)), "clGetPlatformIDs(count)")
    platforms = (C.c_void_p * num_platforms.value)()
    check(cl.clGetPlatformIDs(num_platforms.value, platforms, None), "clGetPlatformIDs(list)")
    platform = platforms[0]
    platform_name = get_str(cl.clGetPlatformInfo, platform, CL_PLATFORM_NAME)

    num_devices = C.c_uint()
    check(cl.clGetDeviceIDs(platform, CL_DEVICE_TYPE_GPU, 0, None, C.byref(num_devices)), "clGetDeviceIDs(count)")
    devices = (C.c_void_p * num_devices.value)()
    check(cl.clGetDeviceIDs(platform, CL_DEVICE_TYPE_GPU, num_devices.value, devices, None), "clGetDeviceIDs(list)")
    device = devices[0]
    device_name = get_str(cl.clGetDeviceInfo, device, CL_DEVICE_NAME)

    err = C.c_int()
    device_obj = C.c_void_p(device)
    ctx = cl.clCreateContext(None, 1, C.byref(device_obj), None, None, C.byref(err))
    check(err.value, "clCreateContext")
    ctx_obj = C.c_void_p(ctx)
    queue = cl.clCreateCommandQueue(ctx_obj, device_obj, 0, C.byref(err))
    check(err.value, "clCreateCommandQueue")

    src = C.c_char_p(KERNEL_SRC.encode())
    length = C.c_size_t(len(KERNEL_SRC))
    program = cl.clCreateProgramWithSource(ctx_obj, 1, C.byref(src), C.byref(length), C.byref(err))
    check(err.value, "clCreateProgramWithSource")
    program_obj = C.c_void_p(program)
    build_err = cl.clBuildProgram(program_obj, 1, C.byref(device_obj), None, None, None)
    if build_err != CL_SUCCESS:
        log = get_build_log(cl, program_obj, device_obj)
        raise RuntimeError(f"clBuildProgram failed: {build_err}\n{log}")

    kernel = cl.clCreateKernel(program_obj, b"bench_v41", C.byref(err))
    check(err.value, "clCreateKernel")
    kernel_obj = C.c_void_p(kernel)

    dataset_buf = cl.clCreateBuffer(ctx_obj, CL_MEM_READ_ONLY, len(dataset), None, C.byref(err))
    check(err.value, "dataset buffer")
    header_buf = cl.clCreateBuffer(ctx_obj, CL_MEM_READ_ONLY, len(header), None, C.byref(err))
    check(err.value, "header buffer")
    anchor_buf = cl.clCreateBuffer(ctx_obj, CL_MEM_READ_ONLY, len(anchor), None, C.byref(err))
    check(err.value, "anchor buffer")

    scratch_words = 256 * 1024 // 8
    scratch_bytes = global_work_size * scratch_words * 8
    scratch_buf = cl.clCreateBuffer(ctx_obj, CL_MEM_READ_WRITE, scratch_bytes, None, C.byref(err))
    check(err.value, "scratch buffer")
    out_buf = cl.clCreateBuffer(ctx_obj, CL_MEM_WRITE_ONLY, global_work_size * 32, None, C.byref(err))
    check(err.value, "out buffer")

    queue_obj = C.c_void_p(queue)
    dataset_obj = C.c_void_p(dataset_buf)
    header_obj = C.c_void_p(header_buf)
    anchor_obj = C.c_void_p(anchor_buf)
    scratch_obj = C.c_void_p(scratch_buf)
    out_obj = C.c_void_p(out_buf)
    check(cl.clEnqueueWriteBuffer(queue_obj, dataset_obj, CL_TRUE, 0, len(dataset), dataset, 0, None, None), "upload dataset")
    check(cl.clEnqueueWriteBuffer(queue_obj, header_obj, CL_TRUE, 0, len(header), header, 0, None, None), "upload header")
    check(cl.clEnqueueWriteBuffer(queue_obj, anchor_obj, CL_TRUE, 0, len(anchor), anchor, 0, None, None), "upload anchor")

    def set_arg(idx, cobj):
        check(cl.clSetKernelArg(kernel_obj, idx, C.sizeof(cobj), C.byref(cobj)), f"set arg {idx}")

    set_arg(0, dataset_obj)
    set_arg(1, C.c_uint(len(dataset) // 64))
    set_arg(2, scratch_obj)
    set_arg(3, C.c_uint(scratch_words))
    set_arg(4, header_obj)
    set_arg(5, C.c_ulonglong(cpu_report["height"]))
    set_arg(6, anchor_obj)
    set_arg(7, C.c_ulonglong(0))
    set_arg(8, C.c_uint(hashes_per_item))
    set_arg(9, out_obj)

    gws = (C.c_size_t * 1)(global_work_size)
    start = time.perf_counter()
    check(cl.clEnqueueNDRangeKernel(queue_obj, kernel_obj, 1, None, gws, None, 0, None, None), "enqueue kernel")
    check(cl.clFinish(queue_obj), "clFinish")
    kernel_s = time.perf_counter() - start

    out = (C.c_ubyte * (global_work_size * 32))()
    check(cl.clEnqueueReadBuffer(queue_obj, out_obj, CL_TRUE, 0, len(out), out, 0, None, None), "read out")
    gpu_hps = (global_work_size * hashes_per_item) / kernel_s
    ratio = gpu_hps / cpu_report["hashes_per_sec"] if cpu_report["hashes_per_sec"] else 0.0

    report = {
        "algo": "dutahash-v4.1",
        "platform": platform_name,
        "device": device_name,
        "dataset_mb": len(dataset) // (1024 * 1024),
        "global_work_size": global_work_size,
        "hashes_per_item": hashes_per_item,
        "total_hashes": global_work_size * hashes_per_item,
        "kernel_elapsed_s": kernel_s,
        "gpu_hashes_per_sec": gpu_hps,
        "cpu_hashes_per_sec": cpu_report["hashes_per_sec"],
        "gpu_to_cpu_ratio": ratio,
        "sample_digest_hex": bytes(out[:32]).hex(),
    }
    (out_dir / "gpu_report.json").write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
