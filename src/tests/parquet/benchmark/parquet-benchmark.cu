/*
 * Copyright 2018 BlazingDB, Inc.
 *     Copyright 2018 Alexander Ocsa <alexander@blazingdb.com>
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

#include <benchmark/benchmark.h>
#include <cassert>
#include <thrust/functional.h>
#include <thrust/device_ptr.h>

#include "column_reader.h"
#include "file_reader.h"

#include "../../helper/utils.cuh"

#ifndef PARQUET_FILE_PATH
#error PARQUET_FILE_PATH must be defined for precompiling
#define PARQUET_FILE_PATH "/"
#endif
 

enum ReaderType : std::uint8_t { kGdf, kParquet };

template <ReaderType T>
struct Readers {};

template <>                                                            
struct Readers<kGdf> {                                                 
    typedef typename gdf::parquet::BoolReader   BoolReader;            
    typedef typename gdf::parquet::Int32Reader  Int32Reader;           
    typedef typename gdf::parquet::Int64Reader  Int64Reader;           
    typedef typename gdf::parquet::FloatReader  FloatReader;          
    typedef typename gdf::parquet::DoubleReader DoubleReader;          
    typedef typename gdf::parquet::FileReader   FileReader;            
     

    static inline gdf_error init_gdf_buffers(void **device_values, gdf_valid_type** host_valid, uint32_t values_malloc_size, gdf_size_type column_size){
        cudaError_t cuda_error = cudaMalloc(device_values, values_malloc_size);
        auto n_bytes = get_number_of_bytes_for_valid(column_size);
        //cudaMalloc((void **)&device_valid, n_bytes);

        *host_valid = (gdf_valid_type*)malloc(n_bytes);
        return GDF_SUCCESS;
    }
     

    static inline gdf_error buffer_to_gdf_column(gdf_column *output, void *device_values, gdf_valid_type* host_valid, uint32_t values_malloc_size, gdf_size_type column_size, gdf_dtype dtype) {
         
        gdf_valid_type *device_valid;
        auto n_bytes = get_number_of_bytes_for_valid(column_size);
        cudaMalloc((void **)&device_valid, n_bytes);
        cudaMemcpy(device_valid, host_valid, n_bytes, cudaMemcpyHostToDevice);

        auto zero_bits = count_zero_bits(host_valid, column_size);
        free(host_valid);

        return gdf_column_view_augmented(output, device_values, device_valid, column_size, dtype, zero_bits);
    }
};


template <>                                                            
struct Readers<kParquet> {                                                 
    typedef typename ::parquet::BoolReader          BoolReader;            
    typedef typename ::parquet::Int32Reader         Int32Reader;           
    typedef typename ::parquet::Int64Reader         Int64Reader;           
    typedef typename ::parquet::FloatReader         FloatReader;          
    typedef typename ::parquet::DoubleReader        DoubleReader;          
    typedef typename ::parquet::ParquetFileReader   FileReader;            

    static inline gdf_error init_gdf_buffers(void **host_values, gdf_valid_type** host_valid, uint32_t values_malloc_size, gdf_size_type column_size){
        *host_values = malloc(values_malloc_size);
         auto n_bytes = get_number_of_bytes_for_valid(column_size);
        *host_valid = (gdf_valid_type*)malloc(n_bytes);
        return GDF_SUCCESS;
    }
    
    static inline gdf_error buffer_to_gdf_column(gdf_column *output, void *host_values, gdf_valid_type* host_valid, uint32_t values_malloc_size, gdf_size_type column_size, gdf_dtype dtype) {
        void *device_values;
        cudaError_t cuda_error = cudaMalloc((void **)&device_values, values_malloc_size);
        cudaMemcpy(device_values, host_values, values_malloc_size, cudaMemcpyHostToDevice);
        
        gdf_valid_type *device_valid;
        auto n_bytes = get_number_of_bytes_for_valid(column_size);
        cudaMalloc((void **)&device_valid, n_bytes);
        cudaMemcpy(device_valid, host_valid, n_bytes, cudaMemcpyHostToDevice);

        auto zero_bits = count_zero_bits(host_valid, column_size);

        free(host_values);
        free(host_valid);
        return gdf_column_view_augmented(output, device_values, device_valid, column_size, dtype, zero_bits);
    }
};


template<parquet::Type::type T>
struct parquet_traits {

};

#define PARQUET_TRAITS_FACTORY(ParquetTypeEnum, ParquetType, GdfTypeValue)  \
template <> struct parquet_traits<ParquetTypeEnum> {                        \
    typedef ParquetType             parquet_type;                           \
    static inline gdf_dtype         gdf_type() { return GdfTypeValue; }       \
}

PARQUET_TRAITS_FACTORY(parquet::Type::BOOLEAN, bool, GDF_INT8);
PARQUET_TRAITS_FACTORY(parquet::Type::INT32, int32_t, GDF_INT32);
PARQUET_TRAITS_FACTORY(parquet::Type::INT64, int64_t, GDF_INT64);
PARQUET_TRAITS_FACTORY(parquet::Type::FLOAT, float, GDF_FLOAT32);
PARQUET_TRAITS_FACTORY(parquet::Type::DOUBLE, double, GDF_FLOAT64);

#undef PARQUET_TRAITS_FACTORY

template <ReaderType T, class ColumnReaderType, parquet::Type::type C>
static inline gdf_error 
convert(gdf_column *column, ColumnReaderType *column_reader, int64_t amount_to_read, uint32_t batch_size) {
    typedef typename parquet_traits<C>::parquet_type    parquet_type;

    parquet_type* values_buffer;
    gdf_valid_type* valid_bits;

    //@todo: amount_to_read check type conversion
    auto values_malloc_size = amount_to_read * sizeof(parquet_type);
    gdf_error status = Readers<T>::init_gdf_buffers((void **)&(values_buffer), &valid_bits, values_malloc_size, amount_to_read);

    std::vector<std::int16_t> definition_level(amount_to_read);
    std::vector<std::int16_t> repetition_level(amount_to_read);
    std::int64_t levels_read;
    std::int64_t values_read = 0;
    std::int64_t nulls_count;

    int64_t rows_read_total = 0;
    while (column_reader->HasNext() && rows_read_total < amount_to_read) {
        int64_t rows_read = column_reader->ReadBatchSpaced(batch_size,
                                     definition_level.data(),
                                     repetition_level.data(),
                                     &values_buffer[rows_read_total],
                                     valid_bits,
                                     0,
                                     &levels_read,
                                     &values_read,
                                     &nulls_count);
        rows_read_total += rows_read;
    }
    Readers<T>::buffer_to_gdf_column(column, (void *)values_buffer, valid_bits, values_malloc_size, amount_to_read, parquet_traits<C>::gdf_type());
    return GDF_SUCCESS;
}


template <ReaderType T>
static inline gdf_error containerFrom(gdf_column *column, std::shared_ptr<parquet::ColumnReader> column_reader, int64_t numRecords, uint32_t batch_size) {

    parquet::Type::type parquetDataType = column_reader->type();

    #define WHEN(dataType, Prefix)                                  \
        if ((dataType) == parquetDataType)                          \
            return convert<T, typename Readers<T>::Prefix##Reader, dataType>       \
                    (column, static_cast<typename Readers<T>::Prefix##Reader*>(column_reader.get()), numRecords, batch_size)

        WHEN(parquet::Type::BOOLEAN, Bool);
        WHEN(parquet::Type::INT32, Int32);
        WHEN(parquet::Type::INT64, Int64);
        WHEN(parquet::Type::FLOAT, Float);
        WHEN(parquet::Type::DOUBLE, Double);

    #undef WHEN

    throw std::invalid_argument("ERROR: Bad parquet column type");
}

void
checkBoolean(const gdf_column &boolean_column) {
    for (std::size_t i = 0; i < boolean_column.size; i++) {
        bool expected = (i % 2) == 0;
        bool value    = static_cast<bool *>(boolean_column.data)[i];

        assert(expected == value);
    }
}

void
checkInt64(const gdf_column &int64_column) {
    for (std::size_t i = 0; i < int64_column.size; i++) {
        std::int64_t expected =
            static_cast<std::int64_t>(i) * 1000000000000;
        std::int64_t value =
            static_cast<std::int64_t *>(int64_column.data)[i];

        assert(expected == value);
    }
}

void
checkDouble(const gdf_column &double_column) {
    for (std::size_t i = 0; i < double_column.size; i++) {
        double expected = i * 0.001;
        double value = static_cast<double *>(double_column.data)[i];
        assert(expected == value);
    }
}
 
void filterops_test(gdf_column* lhs, gdf_column* rhs)
{
    int column_size = lhs->size;
    // print_column<LeftValueType>(lhs);
    // print_column<RightValueType>(rhs);

    gdf_column output = gen_gdb_column<int8_t>(column_size, 0);
    // print_column<int8_t>(&output);

    auto gdf_operator = GDF_EQUALS;

    gdf_error error = gpu_comparison(lhs, rhs, &output, gdf_operator);


    #define WHEN(leftDataType, rightDataType, LeftValueType, RightValueType)                                  \
        if ((leftDataType) == lhs->dtype && (rightDataType) == rhs->dtype) {                          \
            check_column_for_comparison_operation<LeftValueType, RightValueType>(lhs, rhs, &output, gdf_operator); \
            delete_gdf_column(&output); \
            return ; \
        }

        WHEN(GDF_INT8, GDF_INT8, int8_t, int8_t);
        WHEN(GDF_INT8, GDF_INT32, int8_t, int32_t)
        WHEN(GDF_INT8, GDF_INT64, int8_t, int64_t)
        WHEN(GDF_INT8, GDF_FLOAT32, int8_t, float)
        WHEN(GDF_INT8, GDF_FLOAT64, int8_t, double)

        
        
    #undef WHEN
 

}

template <ReaderType T>
inline static void
readRowGroup(const std::unique_ptr<typename Readers<T>::FileReader> &parquet_reader, uint32_t batch_size) {

    std::shared_ptr<parquet::FileMetaData> file_metadata = parquet_reader->metadata();
    const parquet::SchemaDescriptor *schema = file_metadata->schema();
    int numRowGroups = file_metadata->num_row_groups();

    std::vector<gdf_column> columns;

    for (int rowGroupIndex = 0; rowGroupIndex < numRowGroups; rowGroupIndex++) {
        std::shared_ptr<parquet::RowGroupReader> groupReader = parquet_reader->RowGroup(rowGroupIndex);
        const parquet::RowGroupMetaData *rowGroupMetadata = groupReader->metadata();
        for (int columnIndex = 0; columnIndex < file_metadata->num_columns(); columnIndex++) {
            const parquet::ColumnDescriptor *column = schema->Column(columnIndex);
            std::unique_ptr<parquet::ColumnChunkMetaData> columnMetaData = rowGroupMetadata->ColumnChunk(
                    columnIndex);
            parquet::Type::type type = column->physical_type();

            const std::shared_ptr<parquet::ColumnReader> columnReader = groupReader->Column(columnIndex);
            int64_t numRecords = rowGroupMetadata->num_rows();
            // if (columnIndex == 0) {
                gdf_column output;
                containerFrom<T>(&output, columnReader, numRecords, batch_size);
                columns.push_back(output);
            // }
        }
    }  
    
    for(size_t i = 0; i < columns.size(); i++) {
        filterops_test(&columns[0], &columns[i]);
    }
    
    for(size_t i = 0; i < columns.size(); i++)
    {
        delete_gdf_column(&columns[i]);
    }
}

template <ReaderType T>
static void
BM_FileRead(benchmark::State &state) {
    for (auto _ : state) {
        std::unique_ptr<typename Readers<T>::FileReader> reader =
          Readers<T>::FileReader::OpenFile(PARQUET_FILE_PATH);
        
        readRowGroup<T>(reader, state.range(0));
    }
    state.SetBytesProcessed(int64_t(state.iterations()) * int64_t(state.range(0)));
}

// BENCHMARK_TEMPLATE(BM_FileRead, kGdf)->Arg(8)->Arg(64)->Arg(512)->Arg(1<<10)->Arg(8<<10)->Arg(50000);
// BENCHMARK_TEMPLATE(BM_FileRead, kParquet)->Arg(8)->Arg(64)->Arg(512)->Arg(1<<10)->Arg(8<<10)->Arg(50000);

BENCHMARK_TEMPLATE(BM_FileRead, kParquet)->Arg(50000)->Arg(100000)->Arg(500000)->Arg(1000000);
BENCHMARK_TEMPLATE(BM_FileRead, kGdf)->Arg(50000)->Arg(100000)->Arg(500000)->Arg(1000000);