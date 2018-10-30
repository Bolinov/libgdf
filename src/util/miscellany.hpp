/*
 * Copyright 2018 BlazingDB, Inc.
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

/**
 * @file Utility code not yet sorted into other, more specific files in
 * the utility sources directory
 */
#ifndef UTIL_MISCELLANY_HPP_
#define UTIL_MISCELLANY_HPP_

#include <cstdint>

namespace gdf {
namespace util {

/**
 * @brief Determines if an unsigned integer is a power of 2
 *
 * @param[in] x A non-negative integer
 *
 */
template <typename I>
constexpr inline bool is_power_of_two(I x)
{
    static_assert(std::is_integral<I>::value == true, "Invalid type - not an integer");
    return I{0} == ( x & (x-1) );
}


namespace detail {

enum : bool { is_signed = true, is_unsigned = false, isnt_signed = false };

} // namespace detail

template <unsigned NBytes, bool Signed = detail::is_signed>
struct int_t;

template <unsigned NBytes>
using uint_t = typename int_t<NBytes, detail::is_unsigned>::type;

template <unsigned NBytes>
using sint_t = typename int_t<NBytes, detail::is_signed>::type;

template<> struct int_t<1, detail::is_signed   > { using type = int8_t;   };
template<> struct int_t<2, detail::is_signed   > { using type = int16_t;  };
template<> struct int_t<4, detail::is_signed   > { using type = int32_t;  };
template<> struct int_t<8, detail::is_signed   > { using type = int64_t;  };
template<> struct int_t<1, detail::is_unsigned > { using type = uint8_t;  };
template<> struct int_t<2, detail::is_unsigned > { using type = uint16_t; };
template<> struct int_t<4, detail::is_unsigned > { using type = uint32_t; };
template<> struct int_t<8, detail::is_unsigned > { using type = uint64_t; };

} // namespace util
} // namespace gdf

#endif // UTIL_MISCELLANY_HPP_

