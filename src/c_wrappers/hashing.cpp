/*
 * Copyright (c) 2018, BlazingDB Inc.
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

#include <gdf/gdf.h> // for the C API to fullfil
#include <gdf/cpp/operators.hpp> // for the C++ API to use
#include <gdf/cpp/exceptions.hpp>

extern "C" gdf_error gdf_hash(
    int            number_of_columns,
    gdf_column **  columns_to_hash,
    gdf_hash_func  hash_function_type_to_apply,
    gdf_column *   hashes)
{
    constexpr const size_t hash_type_size { 4 };
    using hash_type = util::uint_t<hash_type_size>;

    try {
        auto device = cuda::device::current::get();
        auto stream = device.default_stream();
        auto hashes_ = column::typed<hash_type> { output };
        auto columns_to_hash_ = gsl::span<gdf::column::generic { input, num_cols };

        gdf::elementwise::hash<hash_value_size>(
            stream,
            hashes_,
            columns_to_hash_,
            hash_function_type_to_apply
        );

    } catch(const cuda::runtime_error& e) {
        // Losing information here...
        return GDF_CUDA_ERROR;
    }
    catch(const gdf::runtime_error& e) {
        return e.code();
    }
    catch(const std::exception& e) {
        return GDF_C_ERROR;
    };
    return GDF_SUCCESS;
}

