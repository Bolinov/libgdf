/*
 * Some portions Copyright (c) 2017, Eyal Rozenberg, CWI Amsterdam,
 * adapted from the cuda-api-wrappers repository at
 *
 *   http://github.com/eyalroz/cuda-api-wrappers
 *
 * and are subject to the 3-clause BSD license. You may obtain a copy
 * of the license at
 *
 *   https://github.com/eyalroz/cuda-api-wrappers/blob/master/LICENSE
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * ---
 *
 * Some portions Copyright (c) 2018, BlazingDB Inc., and available under
 * under the Apache License, Version 2.0 (the "License"). You may obtain
 * a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef GDF_EXCEPTIONS_HPP_
#define GDF_EXCEPTIONS_HPP_

#include "types.hpp"

#include <type_traits>
#include <string>
#include <stdexcept>


namespace gdf {


/**
 * @brief The (atrociously over-specific) set of GDF status conditions.
 * It's a copy of @ref gdf_status in class form, to ensure type safety
 */
enum class status : std::underlying_type<gdf_status>::type {
    success                                                          = GDF_SUCCESS,
    error_in_cuda_call                                               = GDF_CUDA_ERROR,
    unsupported_column_element_data_type                             = GDF_UNSUPPORTED_DTYPE,
    size_mismatch_between_columns                                    = GDF_COLUMN_SIZE_MISMATCH,
    colum_size_exceeds_maximum_supported                             = GDF_COLUMN_SIZE_TOO_BIG,
    no_input_data                                                    = GDF_DATASET_EMPTY,
    data_validity_bitmask_missing                                    = GDF_VALIDITY_MISSING,
    operation_does_not_support_columns_with_validity_indicators      = GDF_VALIDITY_UNSUPPORTED,
    invalid_arguments_to_gdf_api_call                                = GDF_INVALID_API_CALL,
    mismatch_between_data_types_of_join_key_column_sequences         = GDF_JOIN_DTYPE_MISMATCH,
    too_many_columns_for_use_as_join_key                             = GDF_JOIN_TOO_MANY_COLUMNS,
    element_data_types_mismatch_between_columns                      = GDF_DTYPE_MISMATCH,
    unsupported_algorithm                                            = GDF_UNSUPPORTED_METHOD,
    invalid_group_by_aggregator_specified                            = GDF_INVALID_AGGREGATOR,
    invalid_hash_function_Type_specified                             = GDF_INVALID_HASH_FUNCTION,
    data_type_mismatch_between_hash_partition_input_and_output       = GDF_PARTITION_DTYPE_MISMATCH,
    insertion_into_hash_table_failed                                 = GDF_HASH_TABLE_INSERT_FAILURE,
    unsupported_join_method                                          = GDF_UNSUPPORTED_JOIN_TYPE,
    requested_profiling_color_is_not_defined                         = GDF_UNDEFINED_NVTX_COLOR,
    null_name_provided_for_profiling_range                           = GDF_NULL_NVTX_NAME,
    c_standard_library_error                                         = GDF_CUDA_ERROR,
    error_processing_the_specified_file                              = GDF_FILE_ERROR
};

// But this really should be...

enum class unused_status {
    success,
    error_in_cuda_call,
    error_in_c_standard_library_call,
    invalid_enum_value, // element data type, aggregator op, hash function type etc.
    unsupported,
    input_columns_incompatible,
    input_columns_incompatible_with_output_columns,
    missing_input_data,
    invalid_column_nullability, // a nullable column passed to an op taking a non-nullable one or vice-versa
    invalid_argument,
    too_many_input_columns,
    unsupported_algorithm                                            = GDF_UNSUPPORTED_METHOD,
    requested_profiling_color_is_not_defined                         = GDF_UNDEFINED_NVTX_COLOR,
    null_pointer, // when non-null was expected
};


constexpr inline bool operator==(const gdf_error& lhs, const status& rhs) { return lhs == static_cast<gdf_error>(rhs);}
constexpr inline bool operator!=(const gdf_error& lhs, const status& rhs) { return lhs != static_cast<gdf_error>(rhs);}
constexpr inline bool operator==(const status& lhs, const gdf_error& rhs) { return static_cast<gdf_error>(lhs) == rhs;}
constexpr inline bool operator!=(const status& lhs, const gdf_error& rhs) { return static_cast<gdf_error>(lhs) != rhs;}

constexpr inline bool is_success(gdf::status status)  { return status == (status_t) status::success; }
constexpr inline bool is_failure(gdf::status status)  { return status != (status_t) status::success; }

/**
 * @brief A class for exceptions raised within the C++ part of GDF. Typically, the C
 * wrapper directly corresponding to libgdf's official API will catch these from
 * C++ code it calls.
 */
class runtime_error : public std::runtime_error {
public:
    ///@cond
    // TODO: Constructor chaining; and perhaps allow for more construction mechanisms?
    runtime_error(gdf::status error_code) :
        std::runtime_error(nullptr),
        code_(error_code)
    { }
    // I wonder if I should do this the other way around
    runtime_error(gdf::status error_code, const std::string& what_arg) :
        std::runtime_error(what_arg),
        code_(error_code)
    { }
    ///@endcond
    runtime_error(gdf_status error_code) :
        runtime_error(static_cast<gdf::status>(error_code)) { }
    runtime_error(gdf_status error_code, const std::string& what_arg) :
        runtime_error(static_cast<gdf::status>(error_code), what_arg) { }

    /**
     * Obtain the GDF status (error) code which resulted in this error being thrown.
     */
    gdf::status code() const { return code_; }

private:
    gdf::status code_;
};


} // namespace gdf

#endif // GDF_EXCEPTIONS_HPP_
