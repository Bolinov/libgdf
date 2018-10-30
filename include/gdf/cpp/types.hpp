#ifndef GDF_TYPES_HPP_
#define GDF_TYPES_HPP_

#include <gsl/span>
#include <cstdint>
#include <typeinfo>

extern "C" {
#include "gdf/cffi/types.h"
}

namespace gdf {

using size  = gdf_size_type;
using index = gdf_index_type;

/**
 * @brief A bit-holder type, used for indicating whether some column elements
 * are null or not. If the corresponding element is null, its value will be 0;
 * otherwise the value is 1 (a "valid" element)
 */
using validity_indicator_type = gdf_valid_type;

namespace detail {

// TODO: We should be using something like Boost's integer.hpp's int<...> types

template <unsigned Bits> struct date_holder_helper     { };
template <>              struct date_holder_helper<32> { using type = gdf_date32; };
template <>              struct date_holder_helper<64> { using type = gdf_date64; };

} // namespace detail

template <unsigned Bits>
using date_holder = typename detail::date_holder_helper<Bits>::type;

using category = gdf_category; // TODO: What's this?

using column_element_type = gdf_dtype;

using status = gdf_error; // TODO: Perhaps make this an enum class

enum class hash_function_type : std::underlying_type<gdf_hash_func>::type {
    murmur_3, identity
};

enum class time_unit : std::underlying_type<gdf_time_unit>::type {
    none         = TIME_UNIT_NONE,
    second       = TIME_UNIT_s,
    millisecond  = TIME_UNIT_ms,
    microsecond  = TIME_UNIT_us,
    nanosecond   = TIME_UNIT_ns
} ;

using time_unit = gdf_time_unit; // TODO: Why not just use std::chrono ?

using extra_element_type_info = gdf_dtype_extra_info;

template <column_element_type ColumnElementType>
struct extra_type_info { };

// TODO: Try to get rid of this in favor of something nicer :-(
// probably a variant.
template <>
struct extra_type_info<GDF_TIMESTAMP> : gdf_dtype_extra_info { };

namespace detail {

static_assert(sizeof(float)  == 4, "float does not have 32 bits");
static_assert(sizeof(double) == 8, "double does not have 64 bits");

template<typename ColumnElementType>
struct column_element_type_to_enum;
template<column_element_type ColumnElementType>
struct column_element_enum_to_type;

template<>
struct column_element_type_to_enum<int8_t          > { constexpr const column_element_type value { INT8 };         };
struct column_element_type_to_enum<int16_t         > { constexpr const column_element_type value { INT16 };        };
struct column_element_type_to_enum<int32_t         > { constexpr const column_element_type value { INT32 };        };
struct column_element_type_to_enum<int64_t         > { constexpr const column_element_type value { INT64 };        };
struct column_element_type_to_enum<float           > { constexpr const column_element_type value { FLOAT };        };
struct column_element_type_to_enum<double          > { constexpr const column_element_type value { FLOAT64 };      };
struct column_element_type_to_enum<date_holder<32> > { constexpr const column_element_type value { DATE32 };       };
struct column_element_type_to_enum<date_holder<64> > { constexpr const column_element_type value { DATE64 };       };
struct column_element_type_to_enum<category        > { constexpr const column_element_type value { GDF_CATEGORY }; };
// struct column_element_type_to_enum<???             > { constexpr const column_element_type value { TIMESTAMP };    };
// struct column_element_type_to_enum<???             > { constexpr const column_element_type value { GDF_STRING };   };

template<>
struct column_element_enum_to_type<INT8         > { using type = int8_t;          };
struct column_element_enum_to_type<INT16        > { using type = int16_t;         };
struct column_element_enum_to_type<INT32        > { using type = int32_t;         };
struct column_element_enum_to_type<INT64        > { using type = int64_t;         };
struct column_element_enum_to_type<FLOAT        > { using type = float;           };
struct column_element_enum_to_type<FLOAT64      > { using type = double;          };
struct column_element_enum_to_type<DATE32       > { using type = date_holder<32>; };
struct column_element_enum_to_type<DATE64       > { using type = date_holder<32>; };
struct column_element_enum_to_type<GDF_CATEGORY > { using type = category;        };
// struct column_element_enum_to_type<TIMESTAMP > { using type = ???              };
// struct column_element_enum_to_type<GDF_STRING> { using type = ???};

// TODO: Use a constexpr array-of-pairs-based structure as the "map"; maybe even use the enum
// value as the index. Alternatively, if we may assume the enum values are contiguous, we can
// just have an std::array
/*
constexpr const inline std::map<std::type_info, gdf_dtype> column_element_type_to_enum_typeinfo = {
        typeid(int8_t),           INT8,
        typeid(int16_t),          INT16,
        typeid(int32_t),          INT32,
        typeid(int64_t),          INT64,
        typeid(float),            FLOAT,
        typeid(double),           FLOAT64,
        typeid(date_holder<32>),  DATE32,
        typeid(date_holder<64>),  DATE64,
        typeid(category),         GDF_CATEGORY,
//        ???, TIMESTAMP,
//        ???, GDF_STRING
};
*/

constexpr const std::array<const std::type_info, 9> column_element_type_to_enum_typeinfo =
{
    typeid(int8_t),
    typeid(int16_t),
    typeid(int32_t),
    typeid(int64_t),
    typeid(float),
    typeid(double),
    typeid(date_holder<32>),
    typeid(date_holder<64>),
    typeid(category),
    // what to put here for TIMESTAMP?
    // what to put here for GDF_STRING?
};

constexpr const std::typeinfo& typeinfo_for(gdf::column_element_type element_type)
{
    return column_element_type_to_enum_typeinfo[static_cast<int>(element_type)];
}

} // namespace detail

namespace column {

class generic;

template <typename T>
class basic_typed {

public: // non-mutator methods
    constexpr column_element_type element_type() const noexcept {
        return detail::column_element_type_to_enum<T>::value;
    }
    constexpr gdf::size size() const noexcept { return data.size_(); }
    constexpr gsl::string_span name() const noexcept { return name_; }
    gsl::span<const T> elements() const noexcept { return elements_; }

public: // mutator methods
    gsl::span<T> elements() noexcept { return elements_; }

public: // constructors and destructor
    constexpr ~basic_typed() = default;
    constexpr basic_typed(
        gsl::span<T>             elements,
        gdf::extra_type_info<T>  extra_type_info,
        gsl::string_span         name) :
        elements_(element), extra_type_info_(extra_type_info), name_(name) { };
    constexpr basic_typed(const basic_typed& other) = default;
    constexpr basic_typed(basic_typed&& other) = default;
    constexpr basic_typed& operator=(basic_typed&& other) = default;
    constexpr basic_typed& operator=(const basic_typed& other) = default;

protected: // data members
     const gsl::span<T>             elements_;
     const gdf::extra_type_info<T>  extra_type_info_;
     const std::string              name_;
          // TODO: Consider making this std::optional<std::string> or
          // a C++11-optional-implementation-version thereof
}; // class basic_typed

/**
 * @brief A typed version of the type-erased @ref column class.
 *
 * @todo: Drop some public members, use methods instead
 *
 * @note `span`s should work in device-side code, despite not having explicit CUDA support,
 * since all of their methods are constexpr'ed. However, that does require compiling with
 * the "--expt-relaxed-constexpr" flag. However, typed
 *
 */
template <typename T, bool Nullable = true>
class typed;

template <typename T>
class typed<T, false> : basic_typed<T> {
public:
    constexpr bool nullable() const noexcept { return false; }
    constexpr gdf::size null_count() const noexcept { return 0; }
};

template <typename T>
class typed<T, true> : basic_typed<T> {
public:
    constexpr bool nullable() const noexcept { return true; }
    constexpr gdf_size null_count() const noexcept { return null_count_; }
    // TODO: Consider a method which returns the null indicators as a non_nullable_typed

public: // constructors and destructor
    constexpr ~typed() = default;
    constexpr typed(
        gsl::span<T>             elements,
        gdf::extra_type_info<T>  extra_type_info,
        gsl::string_span         name,
        gsl::span<validity_indicator_type>
                                 validity_indicators,
        gdf::size                null_count)
    :
        basic_typed(elements, extra_type_info, name),
        validity_indicators_(validity_indicators),
        null_count_(null_count)
    { };
    constexpr typed(const typed& other) = default;
    constexpr typed(typed&& other) = default;
    // TODO: Constructor from a generic column? Maybe a named constructor idiom?
public: // operators
    constexpr typed& operator=(typed&& other) = default;
    constexpr typed& operator=(const basic_typed& other) = default;
    operator generic() const;

protected: // data members
    gsl::span<validity_indicator_type> validity_indicators_;
    size_type null_count_;
};

template <typename T>
using non_nullable_typed = typed<T, false>;

template <typename T>
using nullable_typed = typed<T, true>;

class generic : protected gdf_column {

public: // non-mutator methods
    constexpr column_element_type element_type() const noexcept { return gdf_column::dtype; }
    constexpr gdf::size size() const noexcept { return gdf_column::size; }
    constexpr gsl::string_span name() const noexcept { return gdf_column::col_name; }
    gsl::span<const T> elements() const noexcept { return { elements(), size() }; }
    constexpr bool nullable() const noexcept { return gdf_column::valid != nullptr; }
    constexpr gdf_size null_count() const noexcept { return gdf_column::null_count; }

public: // mutator methods
    gsl::span<T> elements() noexcept { return { elements(), size() }; }
    gsl::span<validity_indicator_type> validity_indicators() noexcept
    {
        assert(nullable() and "Attempt to use the vailidity indicators (= NULL indicators) of a non-nullable column");
        return { gdf_column::valid, size() };
    }

public: // constructors and destructor
    constexpr ~generic() = default;
    using gdf_column::gdf_column; // inherit the constructor, hopefully
    template <typename T>
    constexpr generic(
        gsl::span<T>             elements,
        const char *             name = nullptr)
    :
        gdf_column(
            elements.data(),
            nullptr, // valid
            elements.size(),
            detail::column_element_type_to_enum<T>::value,
            0, // null count
            { }, // extra_type_info
            name
        )
    { };
    constexpr generic(
        gsl::span<T>             elements,
        gsl::span<T>             null_indicators,
        const char *             name = nullptr)
    :
        gdf_column(
            elements.data(),
            nullptr, // valid
            elements.size(),
            detail::column_element_type_to_enum<T>::value,
            0, // null count
            { }, // extra_type_info
            name
        )
    { };

    // TODO: A constructor for timestamp columns - which do use an extra type info

    constexpr generic(const gdf_column& gc) : gdf_column::gdf_column_(gc) { };
    constexpr generic(const gdf_column&& gc) : gdf_column::gdf_column_(gc) { };

    constexpr generic(const generic& other) = default;
    constexpr generic(generic&& other) = default;

public: // operators
    constexpr generic& operator=(generic&& other) = default;
    constexpr generic& operator=(const generic& other) = default;
    template <typename T>
    operator typed_column<T>() const; // Not implemented yet

}; // class generic

/**
 * @brief an alias for the @generic class, emphasizing the choice between
 * baking in the type at compile-time (the @ref gdf::column::typed class),
 * and allowing it to differ at runtime.
 */
using type_erased = generic;


template<typename T>
inline nullable_column<T>::operator generic() const
{
    returb nullable_column<T>{}; // TODO: Rewrite this
}


} // namespace column

// TODO: Confusing and not general, define these elsewhere
// using quantile_method = gdf_quantile_method;

// TODO: Avoid using this. I really doubt anybody using the library should see this.
using nvtx_color = gdf_color;

struct operator_context {
    using algorithm_type = gdf_method;

    bool input_is_sorted; // TODO: But what if there are multiple input columns?
    algorithm_type algorithm;
    bool input_values_are_distinct;
        // TODO: But what if there are multiple input columns?
        // TODO: What about null values? Is data with multiple nulls considered distinct?
    bool producing_sorted_result;
    bool sorting_in_place_allowed;

    operator gdf_context() const noexcept {
        return {
            input_is_sorted, algorithm,
            input_values_are_distinct,
            producing_sorted_result,
            sorting_in_place_allowed
        };
    }

    // TODO: Consider implementing ctors and a ctor from gdf_context
};

//struct _OpaqueIpcParser;
//typedef struct _OpaqueIpcParser gdf_ipc_parser_type;
//
//
//struct _OpaqueRadixsortPlan;
//typedef struct _OpaqueRadixsortPlan gdf_radixsort_plan_type;
//
//
//struct _OpaqueSegmentedRadixsortPlan;
//typedef struct _OpaqueSegmentedRadixsortPlan gdf_segmented_radixsort_plan_type;

namespace sql {

using ordering_type         = order_by_type;
using comparison_operator   = gdf_comparison_operator;
using window_function_type  = ::window_function_type;
using window_reduction_type = window_reduction_type;
using aggregation_type      = gdf_agg_op;

} // namespace sql

} // namespace gdf


#endif // GDF_TYPES_HPP_
