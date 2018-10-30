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

#ifndef GDF_OPERATORS_HPP_
#define GDF_OPERATORS_HPP_

#include "types.hpp"
#include "util/miscellany.hpp"

#include <cuda/api_wrappers.hpp>
#include <array>

namespace gdf {
namespace elementwise {

/**
 * @brief Apply a hash function to each "row" of several columns (with
 * "rows" made up of corresponding elements of each column).
 *
 * @note Output must be disjoint from the inputs, even though __restrict__ is
 * not used
 * @note Currently, NULL values are not supported.
 *
 * @note When passing multiple columns, individual element hash values will
 * be combined using (WRITEME) some default method of hash value combination
 *
 * @note This is an asynchronous function - it only schedules execution
 * to provide the streamless, synchronous libgdf C API, use a wrapper which
 * chooses a stream, call this function, and synchronize with the stream. You will
 * also need to catch exceptions.
 *
 * @tparam NumColumns the number of columns to hash rows from
 * @tparam HashSizeInBytes each row's hash will have be of this size
 *
 * @param stream[in,out] The CUDA stream on which to enqueue the hashing
 * @param hashes[out] The values resulting from applying the appropriate hash function
 * to each of the input rows. This is *not* a hash table!
 * @param columns_to_hash[in] type-erased columns to be hashed; the `i`'th hash is
 * computed using the `i`th elements of each of these columns (which constitute the
 * `i`th row)
 * @param hash_function_type_to_apply[in] Type of hash function to apply to each row
 * @throws CUDA, GDF or standard exceptions due to invalid input or other failures
 *
 */
template <size_t HashSizeInBytes = 4>
void hash(
    cuda::stream_t                                 stream,
    column::typed<util::uint_t<HashSizeInBytes>>   hashes,
    gsl::span<column::generic>                     columns_to_hash,
    hash_function_type                             hash_function_type_to_apply
) noexcept(false);


} // namespace elementwise
} // namespace gdf

#endif // GDF_OPERATORS_HPP_
