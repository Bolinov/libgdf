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

#include <gdf/cpp/operators.hpp>
#include <gdf/cpp/operators.hpp>
#include <cuda/api_wrappers.hpp>


//#include "int_fastdiv.h"
//#include "nvtx_utils.h"

// TODO: Get rid of these:
#include <thrust/tabulate.h>
#include <thrust/device_vector.h>
#include "gdf_table.cuh"
#include "hashmap/hash_functions.cuh"

#include <iostream>
#include <limits>
#include <memory>
#include <algorithm>
#include <utility>

namespace gdf {
namespace operators {

/** 
 * @brief This functor is used to compute the hash value for the rows
 * of a gdf_table
 */
template <template <typename> class hash_function, typename size_type>
struct row_hasher
{
  row_hasher(gdf_table<size_type> const & table_to_hash)
    : the_table{table_to_hash}
  {}

  __device__
  hash_value_type operator()(size_type row_index) const
  {
    return the_table.template hash_row<hash_function>(row_index);
  }

  gdf_table<size_type> const & the_table;
};


template <size_t HashSize>
void hash(
    cuda::stream_t                                 stream,
    column::typed<util::uint_t<HashSizeInBytes>>   hashes,
    gsl::span<column::generic>                     columns_to_hash,
    hash_function_type                             hash_function_type_to_apply
) noexcept(false)
{
    auto verify_input_validity = [&]() {

        static_assert(NumHashedColumns > 0, "Can only hash a positive number of columns");
        // TODO: Use GDF-specific exceptions with GDF error codes embedded in them

        auto nulls_present = std::any_of(
            columns_to_hash.begin(),
            columns_to_hash.end(),
            [](const auto& c) {
                return c.null_count() > 0;
            }
        );
        if (nulls_present) {
            throw std::invalid_arguments("NULL values in hashed columns are not supported.");
        }

        auto output_size = hashes.size();
        std::for_each(
            columns_to_hash.begin(),
            columns_to_hash.end(),
            [&](const auto& col) {
                if (col.size() != output_size) {
                    throw std::invalid_argument("Hash input column length not compatible with output size");
                }
            }
        );

        if (hashes.dtype != detail::column_element_type_to_enum<HashOutputSize>::value) {
            throw std::invalid_argument("hash operator passed an output column of invalid type.");
        }
        auto common_size = output_size;

        if (output_size == 0) { return; }
    };

    verify_input_validity();
    cuda::outstanding_error::ensure_none();

    //-----------------------------------------------------------------------
    // Here starts the ugly part, since we need to interface with gdf_table
    // for now. This part remains to be rewritten

    std::vector<void*> raw_column_data_ptrs { columns_to_hash.size() };
    std::transform(
        columns_to_hash.begin(), columns_to_hash.end(), std::back_inserter(raw_column_data_ptrs),
        [](const column::generic& column) { return const_cast<void*>(column.elements()); }
    );
    // Wrap input columns in gdf_table
    gdf_table<size_type> input_table { &raw_column_data_ptrs[0] };


    // Wrap output buffer in Thrust device_ptr
    using hash_value_type = util::uint_t<HashSize>;
    auto raw_output_buffer = static_cast<hash_value_type*>(hashes.elements().data());
    auto row_hash_values = thrust::device_pointer_cast(raw_output_buffer);

    // Compute the hash value for each row depending on the specified hash function
    switch(hash_function_type_to_apply)
    {
      case hash_function_type::murmur_3:
          thrust::tabulate(
              row_hash_values,
              row_hash_values + common_size,
              row_hasher<MurmurHash3_32,size_type>(input_table));
          break;
      case hash_function_type::identity:
          thrust::tabulate(
              row_hash_values,
              row_hash_values + common_size,
              row_hasher<IdentityHash,size_type>(input_table));
          break;
      // And no need for a default case
    }

    return;
}

// TODO: Implement hash for nullable columns, into a nullable column (?)

} // namespace operators
} // namespace gdf

