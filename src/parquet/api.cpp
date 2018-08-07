/*
 * Copyright 2018 BlazingDB, Inc.
 *     Copyright 2018 Cristhian Alberto Gonzales Castillo <cristhian@blazingdb.com>
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

#include <arrow/util/bit-util.h>
#include <arrow/util/logging.h>
#include <parquet/column_reader.h>
#include <parquet/file/metadata.h>

#include "column_reader.h"
#include "file_reader.h"

#include <gdf/parquet/api.h>

BEGIN_NAMESPACE_GDF_PARQUET

namespace {

template <::parquet::Type::type TYPE>
struct parquet_physical_traits {};

#define PARQUET_PHYSICAL_TRAITS_FACTORY(TYPE, DTYPE)                          \
    template <>                                                               \
    struct parquet_physical_traits<::parquet::Type::TYPE> {                   \
        static constexpr gdf_dtype dtype = GDF_##DTYPE;                       \
    }

PARQUET_PHYSICAL_TRAITS_FACTORY(BOOLEAN, INT8);
PARQUET_PHYSICAL_TRAITS_FACTORY(INT32, INT32);
PARQUET_PHYSICAL_TRAITS_FACTORY(INT64, INT64);
PARQUET_PHYSICAL_TRAITS_FACTORY(INT96, invalid);
PARQUET_PHYSICAL_TRAITS_FACTORY(FLOAT, FLOAT32);
PARQUET_PHYSICAL_TRAITS_FACTORY(DOUBLE, FLOAT64);
PARQUET_PHYSICAL_TRAITS_FACTORY(BYTE_ARRAY, invalid);
PARQUET_PHYSICAL_TRAITS_FACTORY(FIXED_LEN_BYTE_ARRAY, invalid);

#undef PARQUET_PHYSICAL_TRAITS_FACTORY

struct ParquetTypeHash {
    template <class T>
    std::size_t
    operator()(T t) const {
        return static_cast<std::size_t>(t);
    }
};

const std::unordered_map<::parquet::Type::type, gdf_dtype, ParquetTypeHash>
  dtype_from_physical_type_map{
    {::parquet::Type::BOOLEAN, GDF_INT8},
    {::parquet::Type::INT32, GDF_INT32},
    {::parquet::Type::INT64, GDF_INT64},
    {::parquet::Type::INT96, GDF_invalid},
    {::parquet::Type::FLOAT, GDF_FLOAT32},
    {::parquet::Type::DOUBLE, GDF_FLOAT64},
    {::parquet::Type::BYTE_ARRAY, GDF_invalid},
    {::parquet::Type::FIXED_LEN_BYTE_ARRAY, GDF_invalid},
  };

const std::
  unordered_map<::parquet::LogicalType::type, gdf_dtype, ParquetTypeHash>
    dtype_from_logical_type_map{
      {::parquet::LogicalType::NONE, GDF_invalid},
      {::parquet::LogicalType::UTF8, GDF_invalid},
      {::parquet::LogicalType::MAP, GDF_invalid},
      {::parquet::LogicalType::MAP_KEY_VALUE, GDF_invalid},
      {::parquet::LogicalType::LIST, GDF_invalid},
      {::parquet::LogicalType::ENUM, GDF_invalid},
      {::parquet::LogicalType::DECIMAL, GDF_invalid},
      {::parquet::LogicalType::DATE, GDF_DATE32},
      {::parquet::LogicalType::TIME_MILLIS, GDF_invalid},
      {::parquet::LogicalType::TIME_MICROS, GDF_invalid},
      {::parquet::LogicalType::TIMESTAMP_MILLIS, GDF_TIMESTAMP},
      {::parquet::LogicalType::TIMESTAMP_MICROS, GDF_invalid},
      {::parquet::LogicalType::UINT_8, GDF_invalid},
      {::parquet::LogicalType::UINT_16, GDF_invalid},
      {::parquet::LogicalType::UINT_32, GDF_invalid},
      {::parquet::LogicalType::UINT_64, GDF_invalid},
      {::parquet::LogicalType::INT_8, GDF_INT8},
      {::parquet::LogicalType::INT_16, GDF_INT16},
      {::parquet::LogicalType::INT_32, GDF_INT32},
      {::parquet::LogicalType::INT_64, GDF_INT64},
      {::parquet::LogicalType::JSON, GDF_invalid},
      {::parquet::LogicalType::BSON, GDF_invalid},
      {::parquet::LogicalType::INTERVAL, GDF_invalid},
      {::parquet::LogicalType::NA, GDF_invalid},
    };

static inline gdf_dtype
_DTypeFrom(const ::parquet::ColumnDescriptor *const column_descriptor) {
    const ::parquet::LogicalType::type logical_type =
      column_descriptor->logical_type();

    if (logical_type != ::parquet::LogicalType::NONE) {
        return dtype_from_logical_type_map.at(logical_type);
    }

    const ::parquet::Type::type physical_type =
      column_descriptor->physical_type();

    return dtype_from_physical_type_map.at(physical_type);
}

static inline gdf_error
_ReadFile(const std::unique_ptr<FileReader> &file_reader,
          const std::vector<std::size_t> &   indices,
          gdf_column *const                  gdf_columns) {

    const std::size_t num_rows =
      static_cast<std::size_t>(file_reader->metadata()->num_rows());
    const std::size_t num_row_groups =
      static_cast<std::size_t>(file_reader->metadata()->num_row_groups());

    std::int16_t *const definition_levels = new std::int16_t[num_rows];
    std::int16_t *const repetition_levels = new std::int16_t[num_rows];

    for (std::size_t row_group_index = 0; row_group_index < num_row_groups;
         row_group_index++) {
        const std::shared_ptr<::parquet::RowGroupReader> row_group_reader =
          file_reader->RowGroup(static_cast<int>(row_group_index));

        for (std::size_t column_reader_index = 0;
             column_reader_index < indices.size();
             column_reader_index++) {
            const gdf_column &_gdf_column = gdf_columns[column_reader_index];
            const std::shared_ptr<::parquet::ColumnReader> column_reader =
              row_group_reader->Column(
                static_cast<int>(indices[column_reader_index]));

            switch (column_reader->type()) {
#define WHEN(TYPE)                                                            \
    case ::parquet::Type::TYPE:                                               \
        DCHECK_GE(                                                            \
          num_rows,                                                           \
          std::static_pointer_cast<gdf::parquet::ColumnReader<                \
            ::parquet::DataType<::parquet::Type::TYPE>>>(column_reader)       \
            ->ToGdfColumn(                                                    \
              definition_levels, repetition_levels, _gdf_column));            \
        break
                WHEN(BOOLEAN);
                WHEN(INT32);
                WHEN(INT64);
                WHEN(INT96);
                WHEN(FLOAT);
                WHEN(DOUBLE);
                WHEN(BYTE_ARRAY);
                WHEN(FIXED_LEN_BYTE_ARRAY);
            default:
#ifdef GDF_DEBUG
                std::cerr << "Column type error from file" << std::endl;
#endif
                return GDF_IO_ERROR;
#undef WHEN
            }
        }
    }

    delete[] definition_levels;
    delete[] repetition_levels;

    return GDF_SUCCESS;
}

template <::parquet::Type::type TYPE>
static inline gdf_error
_AllocateGdfColumn(const std::size_t                        num_rows,
                   const ::parquet::ColumnDescriptor *const column_descriptor,
                   gdf_column &                             _gdf_column) {
    const std::size_t value_byte_size =
      static_cast<std::size_t>(::parquet::type_traits<TYPE>::value_byte_size);

    try {
        _gdf_column.data =
          static_cast<void *>(new std::uint8_t[num_rows * value_byte_size]);
    } catch (const std::bad_alloc &e) {
#ifdef GDF_DEBUG
        std::cerr << "Allocation error for data\n" << e.what() << std::endl;
#endif
        return GDF_IO_ERROR;
    }

    try {
        _gdf_column.valid = static_cast<gdf_valid_type *>(
          new std::uint8_t[arrow::BitUtil::BytesForBits(num_rows)]);
    } catch (const std::bad_alloc &e) {
#ifdef GDF_DEBUG
        std::cerr << "Allocation error for valid\n" << e.what() << std::endl;
#endif
        return GDF_IO_ERROR;
    }

    _gdf_column.size  = num_rows;
    _gdf_column.dtype = _DTypeFrom(column_descriptor);

    return GDF_SUCCESS;
}

static inline std::vector<const ::parquet::ColumnDescriptor *>
_ColumnDescriptorsFrom(const std::unique_ptr<FileReader> &file_reader,
                       const std::vector<std::size_t> &   indices) {
    const std::shared_ptr<::parquet::RowGroupReader> &row_group_reader =
      file_reader->RowGroup(0);

    std::vector<const ::parquet::ColumnDescriptor *> column_descriptors;
    column_descriptors.reserve(indices.size());

    for (const std::size_t i : indices) {
        column_descriptors.emplace_back(row_group_reader->Column(i)->descr());
    }

    return column_descriptors;
}

static inline gdf_error
_AllocateGdfColumns(const std::unique_ptr<FileReader> &file_reader,
                    const std::vector<std::size_t> &   indices,
                    gdf_column *const                  gdf_columns) {
    const std::vector<const ::parquet::ColumnDescriptor *> column_descriptors =
      _ColumnDescriptorsFrom(file_reader, indices);

    const std::size_t num_columns = indices.size();
    const std::size_t num_rows    = file_reader->metadata()->num_rows();

#define WHEN(TYPE)                                                            \
    case ::parquet::Type::TYPE:                                               \
        _AllocateGdfColumn<::parquet::Type::TYPE>(                            \
          num_rows, column_descriptor, _gdf_column);                          \
        break

    for (std::size_t i = 0; i < num_columns; i++) {
        gdf_column &                             _gdf_column = gdf_columns[i];
        const ::parquet::ColumnDescriptor *const column_descriptor =
          column_descriptors[i];

        switch (column_descriptor->physical_type()) {
            WHEN(BOOLEAN);
            WHEN(INT32);
            WHEN(INT64);
            WHEN(INT96);
            WHEN(FLOAT);
            WHEN(DOUBLE);
            WHEN(BYTE_ARRAY);
            WHEN(FIXED_LEN_BYTE_ARRAY);
        default:
#ifdef GDF_DEBUG
            std::cerr << "Column type not supported" << std::endl;
#endif
            return GDF_IO_ERROR;
        }
    }
#undef WHEN
    return GDF_SUCCESS;
}

static inline gdf_column *
_CreateGdfColumns(const std::size_t num_columns) try {
    return new gdf_column[num_columns];
} catch (const std::bad_alloc &e) {
#ifdef GDF_DEBUG
    std::cerr << "Allocation error for gdf columns\n" << e.what() << std::endl;
#endif
    return nullptr;
}

class ColumnNames {
public:
    explicit ColumnNames(const std::unique_ptr<FileReader> &file_reader) {
        const std::shared_ptr<const ::parquet::FileMetaData> &metadata =
          file_reader->metadata();

        const std::size_t num_columns =
          static_cast<std::size_t>(metadata->num_columns());

        column_names.reserve(num_columns);

        const std::shared_ptr<::parquet::RowGroupReader> row_group_reader =
          file_reader->RowGroup(0);
        for (std::size_t i = 0; i < num_columns; i++) {
            column_names.emplace_back(
              row_group_reader->Column(i)->descr()->name());
        }
    }

    bool
    Contains(std::size_t index) const {
        return index < Size();
    }

    std::size_t
    IndexOf(const std::string &name) const {
        return std::find(column_names.cbegin(), column_names.cend(), name)
               - column_names.cbegin();
    }

    std::size_t
    Size() const {
        return column_names.size();
    }

private:
    std::vector<std::string> column_names;
};

class ColumnFilter {
public:
    explicit ColumnFilter(const char *const *const raw_names) {
        if (raw_names != nullptr) {
            for (const char *const *name_ptr = raw_names; *name_ptr != nullptr;
                 name_ptr++) {
                filter_names.emplace_back(*name_ptr);
            }
        }
    }

    std::vector<std::size_t>
    IndicesFrom(const ColumnNames &column_names) const {
        std::vector<std::size_t> indices;

        if (filter_names.empty()) {
            const std::size_t size = column_names.Size();

            indices.reserve(size);

            for (std::size_t i = 0; i < size; i++) { indices.emplace_back(i); }
        } else {
            const std::size_t size = filter_names.size();

            indices.reserve(size);

            for (std::size_t i = 0; i < size; i++) {
                const std::size_t index =
                  column_names.IndexOf(filter_names[i]);

                if (column_names.Contains(index)) {
                    indices.emplace_back(index);
                }
            }
        }

        return indices;
    }

private:
    std::vector<std::string> filter_names;
};

static inline gdf_error
_CheckMinimalData(const std::unique_ptr<FileReader> &file_reader) {
    const std::shared_ptr<const ::parquet::FileMetaData> &metadata =
      file_reader->metadata();

    if (metadata->num_row_groups() == 0) { return GDF_IO_ERROR; }

    if (metadata->num_rows() == 0) { return GDF_IO_ERROR; }

    return GDF_SUCCESS;
}

static inline std::unique_ptr<FileReader>
_OpenFile(const std::string &filename) try {
    return FileReader::OpenFile(filename);
} catch (std::exception &e) {
#ifdef GDF_DEBUG
    std::cerr << "Open file\n" << e.what() << std::endl;
#endif
    return nullptr;
}

}  // namespace

extern "C" {

gdf_error
read_parquet(const char *const        filename,
             const char *const        engine,
             const char *const *const columns,
             gdf_column **const       out_gdf_columns,
             size_t *const            out_gdf_columns_length) {
    if (engine != nullptr) {
#ifdef GDF_DEBUG
        std::cerr << "Unsupported engine" << std::endl;
#endif
        return GDF_IO_ERROR;
    }

    const std::unique_ptr<FileReader> file_reader = _OpenFile(filename);

    if (!file_reader) { return GDF_IO_ERROR; }

    if (_CheckMinimalData(file_reader) != GDF_SUCCESS) { return GDF_IO_ERROR; }

    const ColumnNames  column_names(file_reader);
    const ColumnFilter column_filter(columns);

    const std::vector<std::size_t> indices =
      column_filter.IndicesFrom(column_names);

    gdf_column *const gdf_columns =
      _CreateGdfColumns(file_reader->metadata()->num_columns());

    if (gdf_columns == nullptr) { return GDF_IO_ERROR; }

    if (_AllocateGdfColumns(file_reader, indices, gdf_columns)
        != GDF_SUCCESS) {
        return GDF_IO_ERROR;
    }

    if (_ReadFile(file_reader, indices, gdf_columns) != GDF_SUCCESS) {
        return GDF_IO_ERROR;
    }

    *out_gdf_columns        = gdf_columns;
    *out_gdf_columns_length = indices.size();

    return GDF_SUCCESS;
}
}

END_NAMESPACE_GDF_PARQUET
