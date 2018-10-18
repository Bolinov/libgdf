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
#include <parquet/metadata.h>

#include <thrust/device_ptr.h>

#include "column_reader.h"
#include "file_reader.h"

#include <gdf/parquet/api.h>
#include <gdf/utils.h>

BEGIN_NAMESPACE_GDF_PARQUET

namespace {

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
    {::parquet::Type::FLOAT, GDF_FLOAT32},
    {::parquet::Type::DOUBLE, GDF_FLOAT64},
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
_ReadColumn(const std::shared_ptr<GdfRowGroupReader> &row_group_reader,
            const std::vector<std::size_t> &          column_indices,
            std::size_t                               offsets[],
            gdf_column *const                         gdf_columns) {
    for (std::size_t column_reader_index = 0;
         column_reader_index < column_indices.size();
         column_reader_index++) {
        const gdf_column &_gdf_column = gdf_columns[column_reader_index];
        const std::shared_ptr<::parquet::ColumnReader> column_reader =
          row_group_reader->Column(
            static_cast<int>(column_indices[column_reader_index]));

        switch (column_reader->type()) {
#define WHEN(TYPE)                                                            \
    case ::parquet::Type::TYPE: {                                             \
        std::shared_ptr<gdf::parquet::ColumnReader<                           \
          ::parquet::DataType<::parquet::Type::TYPE>>>                        \
          reader = std::static_pointer_cast<gdf::parquet::ColumnReader<       \
            ::parquet::DataType<::parquet::Type::TYPE>>>(column_reader);      \
        if (reader->HasNext()) {                                              \
            offsets[column_reader_index] +=                                   \
              reader->ToGdfColumn(_gdf_column, offsets[column_reader_index]); \
        }                                                                     \
    } break
            WHEN(BOOLEAN);
            WHEN(INT32);
            WHEN(INT64);
            WHEN(FLOAT);
            WHEN(DOUBLE);
        default:
#ifdef GDF_DEBUG
            std::cerr << "Column type error from file" << std::endl;
#endif
            return GDF_IO_ERROR;  //TODO: improve using exception handling
#undef WHEN
        }
    }
    return GDF_SUCCESS;
}

/// \brief Get gdf columns from parquet file reader filtered by columns
/// \param[in] file_reader parquet file reader
/// \param[in] row_group_indices to be filtered from file
/// \param[in] column_indices to be filtered from row groups
/// \param[out] gdf_columns with data from columns of parquet file
static inline gdf_error
_ReadFile(const std::unique_ptr<FileReader> &file_reader,
          const std::vector<std::size_t> &   indices,
          gdf_column *const                  gdf_columns) {
    const std::shared_ptr<::parquet::FileMetaData> &metadata =
      file_reader->metadata();
    const std::size_t num_rows =
      static_cast<std::size_t>(metadata->num_rows());
    const std::size_t num_row_groups =
      static_cast<std::size_t>(metadata->num_row_groups());

    std::size_t offsets[indices.size()];
    for (std::size_t i = 0; i < indices.size(); i++) { offsets[i] = 0; }

    for (std::size_t row_group_index = 0; row_group_index < num_row_groups;
         row_group_index++) {
        const auto row_group_reader =
          file_reader->RowGroup(static_cast<int>(row_group_index));

        gdf_error status =
          _ReadColumn(row_group_reader, indices, offsets, gdf_columns);
        if (status != GDF_SUCCESS) { return status; }
    }

    return GDF_SUCCESS;
}

/// \brief Get gdf columns from parquet file reader filtered by columns
///        and row groups
/// \param[in] file_reader parquet file reader
/// \param[in] row_group_indices to be filtered from file
/// \param[in] column_indices to be filtered from row groups
/// \param[out] gdf_columns with data from columns of parquet file
static inline gdf_error
_ReadFile(const std::unique_ptr<FileReader> &file_reader,
          const std::vector<std::size_t> &   row_group_indices,
          const std::vector<std::size_t> &   column_indices,
          gdf_column *const                  gdf_columns) {
    const std::shared_ptr<::parquet::FileMetaData> &metadata =
      file_reader->metadata();
    const std::size_t num_rows =
      static_cast<std::size_t>(metadata->num_rows());
    const std::size_t num_row_groups =
      static_cast<std::size_t>(metadata->num_row_groups());

    std::size_t offsets[column_indices.size()];
    for (std::size_t i = 0; i < column_indices.size(); i++) { offsets[i] = 0; }

    for (const std::size_t row_group_index : row_group_indices) {
        const auto row_group_reader =
          file_reader->RowGroup(static_cast<int>(row_group_index));

        gdf_error status =
          _ReadColumn(row_group_reader, column_indices, offsets, gdf_columns);
        if (status != GDF_SUCCESS) { return status; }
    }

    return GDF_SUCCESS;
}

/// \brief Set artibutes of gdf column getting dtype
//         from parquet column descriptor
/// \param[in] num_rows for gdf column
/// \param[in] column_descriptor of parquet file
/// \param[out] _gdf_column to initialize attibutes
template <::parquet::Type::type TYPE>
static inline gdf_error
_AllocateGdfColumn(const std::size_t                        num_rows,
                   const ::parquet::ColumnDescriptor *const column_descriptor,
                   gdf_column &                             _gdf_column) {
    const std::size_t value_byte_size =
      static_cast<std::size_t>(::parquet::type_traits<TYPE>::value_byte_size);

    cudaError_t status =
      cudaMalloc(&_gdf_column.data, num_rows * value_byte_size);
    if (status != cudaSuccess) {
#ifdef GDF_DEBUG
        std::cerr << "Allocation error for data\n" << e.what() << std::endl;
#endif
        return GDF_IO_ERROR;
    }

    status = cudaMalloc(reinterpret_cast<void **>(&_gdf_column.valid),
                        gdf_get_num_chars_bitmask(num_rows));
    if (status != cudaSuccess) {
#ifdef GDF_DEBUG
        std::cerr << "Allocation error for valid\n" << e.what() << std::endl;
#endif
        return GDF_IO_ERROR;
    }

    _gdf_column.size  = num_rows;
    _gdf_column.dtype = _DTypeFrom(column_descriptor);

    return GDF_SUCCESS;
}

/// \brief Get filtered column descriptors from parquet file reader
/// \param[in] file_reader parquet file reader
/// \param[in] indices of columns in parquet file
/// \return column descriptors
static inline std::vector<const ::parquet::ColumnDescriptor *>
_ColumnDescriptorsFrom(const std::unique_ptr<FileReader> &file_reader,
                       const std::vector<std::size_t> &   indices) {
    const auto &row_group_reader = file_reader->RowGroup(0);

    std::vector<const ::parquet::ColumnDescriptor *> column_descriptors;
    column_descriptors.reserve(indices.size());

    for (const std::size_t i : indices) {
        column_descriptors.emplace_back(row_group_reader->Column(i)->descr());
    }

    return column_descriptors;
}

/// \brief Create gdf columns filtering column indices
/// \param[in] file_reader parquet file reader
/// \param[in] indeces of columns in parquet file
/// \param[out] gdf_column array created
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
            WHEN(FLOAT);
            WHEN(DOUBLE);
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

/// \brief Allocate memory for gdf_columns
/// \param[in] num_columns to allocate
/// \return array of gdf columns
static inline gdf_column *
_CreateGdfColumns(const std::size_t num_columns) try {
    return new gdf_column[num_columns];
} catch (const std::bad_alloc &e) {
#ifdef GDF_DEBUG
    std::cerr << "Allocation error for gdf columns\n" << e.what() << std::endl;
#endif
    return nullptr;
}

//! \brief To manage column names of parquet file reader.
class ColumnNames {
public:

    //! Store in `column_names` std::vector the indexed column names
    /// from the parquet file reader.
    explicit ColumnNames(const std::unique_ptr<FileReader> &file_reader) {
        const std::shared_ptr<const ::parquet::FileMetaData> &metadata =
          file_reader->metadata();

        const std::size_t num_columns =
          static_cast<std::size_t>(metadata->num_columns());

        column_names.reserve(num_columns);

        auto row_group_reader = file_reader->RowGroup(0);
        for (std::size_t i = 0; i < num_columns; i++) {
            column_names.emplace_back(
              row_group_reader->Column(i)->descr()->name());
        }
    }

    //! Check that column index is valid
    bool
    Contains(std::size_t index) const {
        return index < Size();
    }

    //! Get the index of column name
    std::size_t
    IndexOf(const std::string &name) const {
        return std::find(column_names.cbegin(), column_names.cend(), name)
               - column_names.cbegin();
    }

    //! Size of columns in parquet file reader
    std::size_t
    Size() const {
        return column_names.size();
    }

private:
    std::vector<std::string> column_names;
};

//! \brief Tool to filter column names from parquet file.
class ColumnFilter {
public:

    //! Add filter names from raw_names
    explicit ColumnFilter(const char *const *const raw_names) {
        if (raw_names != nullptr) {
            for (const char *const *name_ptr = raw_names; *name_ptr != nullptr;
                 name_ptr++) {
                filter_names.emplace_back(*name_ptr);
            }
        }
    }

    /// Using filter_names from read_parquet(filename, filtering_columns),
    /// get indices from parquet file filtering by column name.
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

//! Ensure that parquet file reader has at least one row group and rows
static inline gdf_error
_CheckMinimalData(const std::unique_ptr<FileReader> &file_reader) {
    const std::shared_ptr<const ::parquet::FileMetaData> &metadata =
      file_reader->metadata();

    if (metadata->num_row_groups() == 0) { return GDF_IO_ERROR; }

    if (metadata->num_rows() == 0) { return GDF_IO_ERROR; }

    return GDF_SUCCESS;
}

//! Get gdf Parquet file reader from filename
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

/// \brief Read parquet file filtering by row_group_indices and column_indices
/// \param[in] filename path
/// \param[in] row_group_indices collection
/// \param[in] column_indices collection
/// \param[out] out_gdf_columns filtered gdf columns
gdf_error
read_parquet_by_ids(const std::string &             filename,
                    const std::vector<std::size_t> &row_group_indices,
                    const std::vector<std::size_t> &column_indices,
                    std::vector<gdf_column *> &     out_gdf_columns) {
    const std::unique_ptr<FileReader> file_reader = _OpenFile(filename);

    if (!file_reader) { return GDF_IO_ERROR; }

    if (_CheckMinimalData(file_reader) != GDF_SUCCESS) { return GDF_IO_ERROR; }

    gdf_column *const gdf_columns = _CreateGdfColumns(column_indices.size());

    if (gdf_columns == nullptr) { return GDF_IO_ERROR; }

    if (_AllocateGdfColumns(file_reader, column_indices, gdf_columns)
        != GDF_SUCCESS) {
        return GDF_IO_ERROR;
    }

    if (_ReadFile(file_reader, row_group_indices, column_indices, gdf_columns)
        != GDF_SUCCESS) {
        return GDF_IO_ERROR;
    }

    for (std::size_t i = 0; i < column_indices.size(); i++) {
        out_gdf_columns.push_back(&gdf_columns[i]);
    }

    return GDF_SUCCESS;
}

extern "C" {

/// \brief Read parquet file into array of gdf columns
/// \param[in] filename path to parquet file
/// \param[in] columns will be read from the file
/// \param[out] out_gdf_columns array
/// \param[out] out_gdf_columns_length number of columns
gdf_error
read_parquet(const char *const        filename,
             const char *const *const columns,
             gdf_column **const       out_gdf_columns,
             size_t *const            out_gdf_columns_length) {
    const std::unique_ptr<FileReader> file_reader = _OpenFile(filename);

    if (!file_reader) { return GDF_IO_ERROR; }

    if (_CheckMinimalData(file_reader) != GDF_SUCCESS) { return GDF_IO_ERROR; }

    const ColumnNames  column_names(file_reader);
    const ColumnFilter column_filter(columns);

    const std::vector<std::size_t> indices =
      column_filter.IndicesFrom(column_names);

    gdf_column *const gdf_columns = _CreateGdfColumns(indices.size());

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
