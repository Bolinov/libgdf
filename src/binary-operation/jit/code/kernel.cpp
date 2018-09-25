/*
 * Copyright 2018-2019 BlazingDB, Inc.
 *     Copyright 2018 Christian Noboa Mardini <christian@blazingdb.com>
 *     Copyright 2018 Rommel Quintanilla <rommel@blazingdb.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace gdf {
namespace binops {
namespace jit {
namespace code {

const char* kernel =
R"***(
    #include <cstdint>
    #include "traits.h"
    #include "operation.h"
    #include "gdf_data.h"

    #define WARP_MASK 0xFFFFFFFF

    __device__ __forceinline__
    uint32_t isValid(int tid, uint32_t* valid, uint32_t mask) {
        return valid[tid / warpSize] & mask;
    }

    __device__ __forceinline__
    void shiftMask(uint32_t& mask) {
        #pragma unroll
        for (int offset = 16; offset > 0; offset /= 2) {
            mask += __shfl_down_sync(WARP_MASK, mask, offset);
        }
    }

    template <typename TypeOut, typename TypeVax, typename TypeVay, typename TypeOpe>
    __global__
    void kernel_v_s(int size, TypeOut* out_data, TypeVax* vax_data, gdf_data vay_data) {
        int tid = threadIdx.x;
        int blkid = blockIdx.x;
        int blksz = blockDim.x;
        int gridsz = gridDim.x;

        int start = tid + blkid * blksz;
        int step = blksz * gridsz;

        for (int i=start; i<size; i+=step) {
            out_data[i] = TypeOpe::template operate<TypeOut, TypeVax, TypeVay>(vax_data[i], (TypeVay)vay_data);
        }
    }

    template <typename TypeOut, typename TypeVax, typename TypeVay, typename TypeOpe>
    __global__
    void kernel_v_v(int size, TypeOut* out_data, TypeVax* vax_data, TypeVay* vay_data) {
        int tid = threadIdx.x;
        int blkid = blockIdx.x;
        int blksz = blockDim.x;
        int gridsz = gridDim.x;

        int start = tid + blkid * blksz;
        int step = blksz * gridsz;

        for (int i=start; i<size; i+=step) {
            out_data[i] = TypeOpe::template operate<TypeOut, TypeVax, TypeVay>(vax_data[i], vay_data[i]);
        }
    }

    template <typename TypeOut, typename TypeVax, typename TypeVay, typename TypeVal, typename TypeOpe>
    __global__
    void kernel_v_s_d(int size, gdf_data def_data,
                      TypeOut* out_data, TypeVax* vax_data, gdf_data vay_data,
                      uint32_t* out_valid, uint32_t* vax_valid) {
        int tid = threadIdx.x;
        int blkid = blockIdx.x;
        int blksz = blockDim.x;
        int gridsz = gridDim.x;

        int start = tid + blkid * blksz;
        int step = blksz * gridsz;

        for (int i=start; i<size; i+=step) {
            uint32_t mask = 1 << (i % warpSize);
            uint32_t is_vax_valid = isValid(i, vax_valid, mask);

            TypeVax vax_data_aux = vax_data[i];
            if ((is_vax_valid & mask) != mask) {
                vax_data_aux = (TypeVal)def_data;
            }

            out_data[i] = TypeOpe::template operate<TypeOut, TypeVax, TypeVay>(vax_data_aux, (TypeVay)vay_data);

            shiftMask(mask);

            if ((i % warpSize) == 0) {
                out_valid[i / warpSize] = mask;
            }
        }
    }


    template <typename TypeOut, typename TypeVax, typename TypeVay, typename TypeVal, typename TypeOpe>
    __global__
    void kernel_v_v_d(int size, gdf_data def_data,
                      TypeOut* out_data, TypeVax* vax_data, TypeVay* vay_data,
                      uint32_t* out_valid, uint32_t* vax_valid, uint32_t* vay_valid) {
        int tid = threadIdx.x;
        int blkid = blockIdx.x;
        int blksz = blockDim.x;
        int gridsz = gridDim.x;

        int start = tid + blkid * blksz;
        int step = blksz * gridsz;

        for (int i=start; i<size; i+=step) {
            uint32_t mask = 1 << (i % warpSize);
            uint32_t is_vax_valid = isValid(i, vax_valid, mask);
            uint32_t is_vay_valid = isValid(i, vay_valid, mask);

            TypeVax vax_data_aux = vax_data[i];
            TypeVay vay_data_aux = vay_data[i];
            if ((is_vax_valid & mask) != mask) {
                vax_data_aux = (TypeVal)def_data;
            }
            else if ((is_vay_valid & mask) != mask) {
                vay_data_aux = (TypeVal)def_data;
            }
            if ((is_vax_valid | is_vay_valid) == mask) {
                out_data[i] = TypeOpe::template operate<TypeOut, TypeVax, TypeVay>(vax_data_aux, vay_data_aux);
            } else {
                mask = 0;
            }

            shiftMask(mask);

            if ((i % warpSize) == 0) {
                out_valid[i / warpSize] = mask;
            }
        }
    }
)***";

} // code
} // jit
} // binops
} // gdf
